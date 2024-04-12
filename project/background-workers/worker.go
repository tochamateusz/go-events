package backgroundworkers

import (
	"context"
	"tickets/clients"
)

type Worker struct {
	queue chan Message

	receiptsClient     clients.ReceiptsClient
	spreadsheetsClient clients.SpreadsheetsClient
}

func NewWorker(receiptsClient clients.ReceiptsClient, spreadsheetsClient clients.SpreadsheetsClient) *Worker {
	return &Worker{
		queue:              make(chan Message, 100),
		receiptsClient:     receiptsClient,
		spreadsheetsClient: spreadsheetsClient,
	}
}

func (w *Worker) Send(msg ...Message) {
	for _, m := range msg {
		w.queue <- m
	}
}

func (w *Worker) Run() {
	for msg := range w.queue {
		switch msg.Task {
		case TaskIssueReceipt:
			{
				err := w.receiptsClient.IssueReceipt(context.Background(), msg.TicketID)
				if err != nil {
					w.Send(msg)
					continue
				}
				break
			}
		case TaskAppendToTracker:
			{
				err := w.spreadsheetsClient.AppendRow(context.Background(), "tickets-to-print", []string{msg.TicketID})
				if err != nil {
					w.Send(msg)
					continue
				}
			}
		}
	}
}
