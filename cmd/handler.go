package cmd

import (
	"fmt"
	"log"

	"github.com/matrixbotio/go-common-lib/logger"

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
		Logger:                  logger.NewLogger(myLogger{}, "host", "source", "0"),
	}

	h, err := rmqworker.NewRMQHandler(task)
	if err != nil {
		panic(fmt.Sprintf("error: %#v", err))
	}

	return h
}

type myLogger struct {
}

func (l myLogger) Send(data string) {
	log.Println(data)
}