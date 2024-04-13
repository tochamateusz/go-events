package backgroundworkers

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"tickets/clients"
	"tickets/tickets"

	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

type Worker struct {
	receiptsClient     clients.ReceiptsClient
	spreadsheetsClient clients.SpreadsheetsClient

	publisher *redisstream.Publisher
	router    *message.Router
}

type Message struct {
	Task   Task
	Ticket tickets.Ticket
}

func (w *Worker) IssueReceiptHandler(msg *message.Message) error {
	ticket := tickets.Ticket{}
	err := json.Unmarshal(msg.Payload, &ticket)
	if err != nil {
		return err
	}

	err = w.receiptsClient.IssueReceipt(
		msg.Context(),
		clients.IssueReceiptRequest{
			TicketID: ticket.TicketId,
			Price: clients.Price{
				Amount:   ticket.Price.Amount,
				Currency: ticket.Price.Currency,
			},
		})
	return err
}

func (w *Worker) AppendToTrackerHandler(msg *message.Message) error {
	ticket := tickets.Ticket{}
	err := json.Unmarshal(msg.Payload, &ticket)
	if err != nil {
		return err
	}

	err = w.spreadsheetsClient.AppendRow(
		msg.Context(),
		"tickets-to-print",
		[]string{
			ticket.TicketId,
			ticket.CustomerEmail,
			ticket.Price.Amount,
			ticket.Price.Currency,
		})

	return err
}

func NewWorker(
	receiptsClient clients.ReceiptsClient,
	spreadsheetsClient clients.SpreadsheetsClient,

	watermillLogger *log.WatermillLogrusAdapter,
	router *message.Router,
) *Worker {
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

	worker := &Worker{
		publisher: publisher,
		router:    router,

		receiptsClient:     receiptsClient,
		spreadsheetsClient: spreadsheetsClient,
	}

	router.AddNoPublisherHandler("issue-receipt-handler", "issue-receipt", issuerSub, worker.IssueReceiptHandler)
	router.AddNoPublisherHandler("append-to-tracker-handler", "append-to-tracker", taskTrackerSub, worker.AppendToTrackerHandler)

	return worker
}

type Price struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type IssueReceiptPayload struct {
	TicketId string `json:"ticket_id"`
	Price    Price  `json:"price"`
}
type PrintTicketPayload struct {
	TicketId      string `json:"ticket_id"`
	CustomerEmail string `json:"customer_email"`
	Price         Price  `json:"price"`
}

func (w *Worker) Send(msg Message) {

	if msg.Task == TaskIssueReceipt {

		issueReceiptPayload := IssueReceiptPayload{
			TicketId: msg.Ticket.TicketId,
			Price: Price{
				Amount:   msg.Ticket.Price.Amount,
				Currency: msg.Ticket.Price.Currency,
			},
		}

		payload, err := json.Marshal(issueReceiptPayload)
		if err != nil {
			return
		}
		msg := message.NewMessage(watermill.NewUUID(), payload)
		w.publisher.Publish("issue-receipt", msg)
	}

	if msg.Task == TaskAppendToTracker {
		printTicketPayload := PrintTicketPayload{
			TicketId:      msg.Ticket.TicketId,
			CustomerEmail: msg.Ticket.CustomerEmail,
			Price: Price{
				Amount:   msg.Ticket.Price.Amount,
				Currency: msg.Ticket.Price.Currency,
			},
		}

		payload, err := json.Marshal(printTicketPayload)
		if err != nil {
			return
		}

		msg := message.NewMessage(watermill.NewUUID(), payload)
		w.publisher.Publish("append-to-tracker", msg)
	}

}

func (w *Worker) Run() {
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	gr, ctx := errgroup.WithContext(ctx)

	gr.Go(func() error {
		return w.router.Run(context.Background())
	})

	err := gr.Wait()
	if err != nil {
		panic(err)
	}
	<-ctx.Done()
}
