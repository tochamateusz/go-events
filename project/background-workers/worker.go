package backgroundworkers

import (
	"context"
	"os"
	"tickets/clients"

	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type Worker struct {
	receiptsClient     clients.ReceiptsClient
	spreadsheetsClient clients.SpreadsheetsClient

	issuerSub      *redisstream.Subscriber
	taskTrackerSub *redisstream.Subscriber

	publisher *redisstream.Publisher
}

func NewWorker(
	receiptsClient clients.ReceiptsClient,
	spreadsheetsClient clients.SpreadsheetsClient,
) *Worker {
	watermillLogger := log.NewWatermill(logrus.NewEntry(logrus.StandardLogger()))

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	issuerSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "issue-receipt",
	}, watermillLogger)
	if err != nil {
		panic("cant creat sub")
	}

	taskTrackerSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "append-to-tracker",
	}, watermillLogger)
	if err != nil {
		panic("cant creat sub")
	}

	publisher, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client: rdb,
	}, watermillLogger)
	if err != nil {
		panic(err)
	}

	return &Worker{
		receiptsClient:     receiptsClient,
		spreadsheetsClient: spreadsheetsClient,
		issuerSub:          issuerSub,
		taskTrackerSub:     taskTrackerSub,
		publisher:          publisher,
	}
}

func (w *Worker) Send(msg Message) {
	if msg.Task == TaskIssueReceipt {
		msg := message.NewMessage(watermill.NewUUID(), []byte(msg.TicketID))
		w.publisher.Publish("issue-receipt", msg)
	}

	if msg.Task == TaskAppendToTracker {
		msg := message.NewMessage(watermill.NewUUID(), []byte(msg.TicketID))
		w.publisher.Publish("append-to-tracker", msg)
	}

}

func (w *Worker) Run() {
	go func() {
		messages, err := w.issuerSub.Subscribe(context.Background(), "issue-receipt")
		if err != nil {
			panic(err)
		}

		for msg := range messages {
			payload := string(msg.Payload)
			err := w.receiptsClient.IssueReceipt(context.Background(), payload)
			if err != nil {
				msg.Nack()
			} else {
				msg.Ack()
			}
		}
	}()

	go func() {
		messages, err := w.taskTrackerSub.Subscribe(context.Background(), "append-to-tracker")
		if err != nil {
			panic(err)
		}

		for msg := range messages {
			payload := string(msg.Payload)
			err := w.spreadsheetsClient.AppendRow(context.Background(), "tickets-to-print", []string{payload})
			if err != nil {
				msg.Nack()
			} else {
				msg.Ack()
			}
		}
	}()

}
