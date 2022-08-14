package cmd

import (
	"fmt"

	"github.com/matrixbotio/rmqworker-lib"
)

func GetHandler() *rmqworker.RMQHandler {
	task := rmqworker.CreateRMQHandlerTask{
		Data: rmqworker.RMQConnectionData{
			User:     "example",
			Password: "example",
			Host:     "localhost",
			Port:     "5672",
			UseTLS:   false,
		},
		UseErrorCallback:        false,
		ConnectionErrorCallback: nil,
	}

	h, err := rmqworker.NewRMQHandler(task)
	if err != nil {
		panic(fmt.Sprintf("error: %#v", err))
	}

	return h
}
