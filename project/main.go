package main

import (
	"net/http"
	"os"
	backgroundworkers "tickets/background-workers"
	externalClients "tickets/clients"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
)

type TicketsConfirmationRequest struct {
	Tickets []string `json:"tickets"`
}

func main() {
	log.Init(logrus.InfoLevel)

	clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
	if err != nil {
		panic(err)
	}

	receiptsClient := externalClients.NewReceiptsClient(clients)
	spreadsheetsClient := externalClients.NewSpreadsheetsClient(clients)

	e := commonHTTP.NewEcho()

	w := backgroundworkers.NewWorker(receiptsClient, spreadsheetsClient)
	go w.Run()

	e.POST("/tickets-confirmation", func(c echo.Context) error {
		var request TicketsConfirmationRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {

			w.Send(backgroundworkers.Message{
				Task:     backgroundworkers.TaskIssueReceipt,
				TicketID: ticket,
			})

			w.Send(backgroundworkers.Message{
				Task:     backgroundworkers.TaskAppendToTracker,
				TicketID: ticket,
			})

		}

		return c.NoContent(http.StatusOK)
	})

	logrus.Info("Server starting...")

	err = e.Start(":8080")
	if err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}
