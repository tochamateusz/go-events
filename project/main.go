package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	backgroundworkers "tickets/background-workers"
	externalClients "tickets/clients"
	"tickets/ports"

	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

func main() {
	log.Init(logrus.InfoLevel)

	clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
	if err != nil {
		panic(err)
	}

	receiptsClient := externalClients.NewReceiptsClient(clients)
	spreadsheetsClient := externalClients.NewSpreadsheetsClient(clients)

	watermillLogger := log.NewWatermill(logrus.NewEntry(logrus.StandardLogger()))

	router, err := message.NewRouter(message.RouterConfig{}, watermillLogger)
	if err != nil {
		panic(err)
	}

	w := backgroundworkers.NewWorker(receiptsClient, spreadsheetsClient, watermillLogger, router)
	httpPort := ports.NewHttpPort(w)
	go w.Run()

	e := commonHTTP.NewEcho()
	e.GET("/health", httpPort.Health)
	// e.POST("/tickets-status", httpPort.TicketsStatus)

	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	gr, ctx := errgroup.WithContext(ctx)

	gr.Go(func() error {
		<-router.Running()
		logrus.Info("Server starting...")
		err := e.Start(":8080")
		if err != nil && err != http.ErrServerClosed {
			return err
		}

		return nil
	})

	err = gr.Wait()
	if err != nil {
		panic(err)
	}
	<-ctx.Done()

}
