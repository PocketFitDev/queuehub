package main

import (
	"context"
	"log"

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
	err = q.Produce(context.Background(), MessageExample{Data: "sosi pisku"})
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Published succussfully!")
}
