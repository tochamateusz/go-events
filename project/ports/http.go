package ports

import (
	"encoding/json"
	"net/http"
	backgroundworkers "tickets/background-workers"
	"tickets/tickets"

	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
)

type HttpPort struct {
	w *backgroundworkers.Worker
}

func NewHttpPort(w *backgroundworkers.Worker) HttpPort {
	return HttpPort{
		w,
	}

}

type TicketsStatusRequest struct {
	Tickets []tickets.Ticket `json:"tickets"`
}

func (h *HttpPort) TicketsStatus(c echo.Context) error {
	correlationId := c.Request().Header.Get("Correlation-ID")

	ticketsStatusRequest := TicketsStatusRequest{}
	err := json.NewDecoder(c.Request().Body).Decode(&ticketsStatusRequest)
	if err != nil {
		logrus.Error(err)
		return err
	}

	for _, ticket := range ticketsStatusRequest.Tickets {
		h.w.Send(backgroundworkers.Message{
			CorrelationId: correlationId,
			Ticket:        ticket,
		})

	}

	return c.NoContent(http.StatusOK)
}

func (h *HttpPort) Health(c echo.Context) error {
	return c.String(http.StatusOK, "ok")
}
