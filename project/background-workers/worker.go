package backgroundworkers

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"tickets/clients"
	"tickets/tickets"
	"time"

	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/google/uuid"
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
	CorrelationId string
	Ticket        tickets.Ticket
}

func (w *Worker) issueReceiptHandler(msg *message.Message) error {
	event := TicketEvent{}
	err := json.Unmarshal(msg.Payload, &event)
	if err != nil {
		return err
	}

	err = w.receiptsClient.IssueReceipt(
		msg.Context(),
		clients.IssueReceiptRequest{
			TicketID: event.TicketId,
			Price: clients.Price{
				Amount:   event.Price.Amount,
				Currency: event.Price.Currency,
			},
		})
	return err
}

func (w *Worker) bookingCanceled(msg *message.Message) error {
	event := TicketEvent{}
	err := json.Unmarshal(msg.Payload, &event)
	if err != nil {
		return err
	}

	err = w.spreadsheetsClient.AppendRow(
		msg.Context(),
		"tickets-to-refund",
		[]string{
			event.TicketId,
			event.CustomerEmail,
			event.Price.Amount,
			event.Price.Currency,
		})

	return err
}

func (w *Worker) bookingConfirmed(msg *message.Message) error {
	event := TicketEvent{}
	err := json.Unmarshal(msg.Payload, &event)
	if err != nil {
		return err
	}

	err = w.spreadsheetsClient.AppendRow(
		msg.Context(),
		"tickets-to-print",
		[]string{
			event.TicketId,
			event.CustomerEmail,
			event.Price.Amount,
			event.Price.Currency,
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

	confirmedBookingSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "append-to-tracker",
	}, watermillLogger)
	if err != nil {
		panic("cant creat sub")
	}

	canceledBookingSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
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

	router.AddNoPublisherHandler("issue-receipt-handler", TicketBookingConfirmed, issuerSub, worker.issueReceiptHandler)
	router.AddNoPublisherHandler("ticket-booking-confirmed", TicketBookingConfirmed, confirmedBookingSub, worker.bookingConfirmed)
	router.AddNoPublisherHandler("ticket-booking-canceled", TicketBookingCanceled, canceledBookingSub, worker.bookingCanceled)

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

type Header struct {
	Id          string `json:"id"`
	PublishedAt string `json:"published_at"`
}

type Meta struct {
	CorrelationId string `json:"correlation_id"`
}

func NewHeader() Header {
	return Header{
		Id:          uuid.NewString(),
		PublishedAt: time.Now().Format(time.RFC3339),
	}
}

var TicketBookingConfirmed = "TicketBookingConfirmed"
var TicketBookingCanceled = "TicketBookingCanceled"

type TicketEvent struct {
	Header        Header `json:"header"`
	Meta          Meta   `json:"meta"`
	TicketId      string `json:"ticket_id"`
	CustomerEmail string `json:"customer_email"`
	Price         Price  `json:"price"`
}

func (w *Worker) Send(msg Message) {

	ticketEvent := TicketEvent{
		Header:        NewHeader(),
		Meta:          Meta{CorrelationId: msg.CorrelationId},
		TicketId:      msg.Ticket.TicketId,
		CustomerEmail: msg.Ticket.CustomerEmail,
		Price: Price{
			Amount:   msg.Ticket.Price.Amount,
			Currency: msg.Ticket.Price.Currency,
		},
	}

	payload, err := json.Marshal(ticketEvent)
	if err != nil {
		return
	}

	borkerMsg := message.NewMessage(watermill.NewUUID(), payload)
	middleware.SetCorrelationID(msg.CorrelationId, borkerMsg)

	if msg.Ticket.Status == "confirmed" {
		w.publisher.Publish(TicketBookingConfirmed, borkerMsg)
	}

	if msg.Ticket.Status == "canceled" {
		w.publisher.Publish(TicketBookingCanceled, borkerMsg)
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
