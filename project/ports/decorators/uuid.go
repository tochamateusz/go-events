package decorators

import (
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

func UUID(next message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) (msgs []*message.Message, err error) {
		uuidString := uuid.NewString()
		logger := log.FromContext(msg.Context())

		defer func() {
			if err != nil {
				logger.WithField("message_uuid", uuidString).
					WithField("error", err).
					Info("Message handling error")
			}
		}()
		logger.WithField("message_uuid", uuidString).Info("Handling a message")
		return next(msg)
	}
}
