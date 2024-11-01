package nats

import (
	"context"
	"crypto"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	queuehub "github.com/PocketFitDev/queuehub/interface"
	nats "github.com/nats-io/nats.go"
)

type Config struct {
	Storage            RedeliveryStorage
	MaxRedeliveryCount int64
	BatchSize          int64
	ConnectionDSN      string
	QueueName          string
}

type QueuesClient[T any] struct {
	Config
	js nats.JetStreamContext
}

func New[T any](cfg Config) (queuehub.QueuesClient[T], error) {
	conn, err := nats.Connect(cfg.ConnectionDSN)
	if err != nil {
		return nil, err
	}
	js, err := conn.JetStream(nats.PublishAsyncMaxPending(int(cfg.BatchSize)))
	if err != nil {
		return nil, err
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     cfg.QueueName,
		Subjects: []string{cfg.QueueName},
	})
	if err != nil {
		return nil, err
	}
	return &QueuesClient[T]{
		Config: cfg,
		js:     js,
	}, nil
}

func (q *QueuesClient[T]) Consume(ctx context.Context, handler queuehub.ConsumerFunc[T]) error {
	_, err := q.js.AddConsumer(q.QueueName, &nats.ConsumerConfig{
		Durable:       q.QueueName,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxAckPending: -1,
	})
	if err != nil {
		return err
	}

	sub, err := q.js.PullSubscribe(
		q.QueueName,
		q.QueueName,
		nats.ManualAck(),
		nats.Bind(q.QueueName, q.QueueName),
		nats.Context(ctx),
	)
	if err != nil {
		return err
	}

	for {
		batch, err := sub.Fetch(int(q.BatchSize))
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			return err
		}
		for _, msgRaw := range batch {
			key := crypto.MD5.New().Sum(msgRaw.Data)
			attempt, err := q.Storage.Attempt(fmt.Sprintf("%x", key))
			if err != nil {
				return err
			}
			if q.MaxRedeliveryCount != -1 && attempt >= q.MaxRedeliveryCount {
				err = msgRaw.Term()
				if err != nil {
					return err
				}
				continue
			}
			var msg T
			err = json.Unmarshal(msgRaw.Data, &msg)
			if err != nil {
				err = msgRaw.Term()
				if err != nil {
					return err
				}
				continue
			}
			result, err := handler(ctx, msg, &queuehub.Meta{
				AttemptNumber: attempt,
			})
			if err != nil {
				return err
			}
			switch result {
			case queuehub.ACK:
				err := msgRaw.Ack()
				if err != nil {
					return err
				}
			case queuehub.NACK:
				err := msgRaw.Nak()
				if err != nil {
					return err
				}
			case queuehub.DEFER:
				delay := time.Duration(int64(time.Second) * (attempt + 1))
				err := msgRaw.NakWithDelay(delay)
				if err != nil {
					return err
				}
			}

		}

	}
}

func (q *QueuesClient[T]) Produce(_ context.Context, msg T) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = q.js.Publish(q.QueueName, bytes)
	return err
}
