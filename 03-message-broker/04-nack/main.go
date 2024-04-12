package main

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

type AlarmClient interface {
	StartAlarm() error
	StopAlarm() error
}

func ConsumeMessages(sub message.Subscriber, alarmClient AlarmClient) {
	handlers := map[string](func() error){
		"0": alarmClient.StopAlarm,
		"1": alarmClient.StartAlarm,
	}

	messages, err := sub.Subscribe(context.Background(), "smoke_sensor")
	if err != nil {
		panic(err)
	}

	for msg := range messages {
		smoke_detected := string(msg.Payload)

		handler, ok := handlers[smoke_detected]
		if ok == false {
			continue
		}

		err := handler()
		if err != nil {
			msg.Nack()
			continue
		}

		msg.Ack()
	}

}
