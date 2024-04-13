package decorators

import (
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/sirupsen/logrus"
)

func CorrelationID(next message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {

		correlationID := msg.Metadata.Get("correlation_id")
		ctx := log.ContextWithCorrelationID(msg.Context(), correlationID)
		ctx = log.ToContext(ctx, logrus.WithFields(logrus.Fields{"correlation_id": correlationID}))
		msg.SetContext(ctx)

		return next(msg)
	}
}
