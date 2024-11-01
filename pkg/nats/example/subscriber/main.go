package main

import (
	"context"
	"log"

	queuehub "github.com/PocketFitDev/queuehub/interface"
	"github.com/PocketFitDev/queuehub/pkg/nats"
)

type MessageExample struct {
	Data string
}

func main() {

	cfg := nats.Config{
		Storage:            nats.NewInMem(),
		MaxRedeliveryCount: 5,
		BatchSize:          32,
		QueueName:          "test",
		ConnectionDSN:      "localhost:4222",
	}

	q, err := nats.New[MessageExample](cfg)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Starting consumer...")
	err = q.Consume(context.Background(), func(ctx context.Context, msg MessageExample, meta *queuehub.Meta) (queuehub.Result, error) {
		log.Println("Received message:", msg, "Attempts:", meta.AttemptNumber)
		return queuehub.DEFER, nil
	})

	log.Fatalln(err)
}
