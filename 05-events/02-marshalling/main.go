package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
)

type PaymentCompleted struct {
	PaymentID   string `json:"payment_id"`
	OrderID     string `json:"order_id"`
	CompletedAt string `json:"completed_at"`
}

type OrderConfirmed struct {
	OrderID     string `json:"order_id"`
	ConfirmedAt string `json:"confirmed_at"`
}

func main() {
	logger := watermill.NewStdLogger(false, false)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	sub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client: rdb,
	}, logger)
	if err != nil {
		panic(err)
	}

	pub, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client: rdb,
	}, logger)
	if err != nil {
		panic(err)
	}

	router.AddHandler(
		"handler",
		"payment-completed",
		sub,
		"order-confirmed",
		pub,
		func(msg *message.Message) ([]*message.Message, error) {
			paymentCompleted := PaymentCompleted{}
			err := json.Unmarshal(msg.Payload, &paymentCompleted)
			if err != nil {
				return nil, err
			}

			orderConfirmed := OrderConfirmed{}
			orderConfirmed.OrderID = paymentCompleted.OrderID
			orderConfirmed.ConfirmedAt = paymentCompleted.CompletedAt

			payload, err := json.Marshal(orderConfirmed)
			if err != nil {
				return nil, err
			}

			msg.Payload = payload
			return []*message.Message{msg}, nil
		},
	)

	err = router.Run(context.Background())
	if err != nil {
		panic(err)
	}
}
