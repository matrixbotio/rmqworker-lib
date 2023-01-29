package main

import (
	"context"
	"encoding/json"

	"github.com/matrixbotio/go-common-lib/zes"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/pkg/syncrpc"
)

const (
	requestsExchange  = "some_external_service_that_does_some_work"
	responsesExchange = "some_external_service_that_does_some_work.response"
)

type outgoingData struct {
	MagicNumber int
}

type incomingData struct {
	ResponseText string
}

func main() {
	logger, loggerErr := zes.Init(false)
	if loggerErr != nil {
		panic(loggerErr)
	}
	defer logger.Close()
	zap.ReplaceGlobals(logger.New(zap.InfoLevel))

	rmqHandler := getHandler()

	zap.L().Info("Started")
	zap.L().Info("Sending message")

	handlerProps := syncrpc.HandlerProps{
		RequestsExchange:  requestsExchange,
		ResponsesExchange: responsesExchange,
		ServiceTag:        "service-1",
	}

	handler, err := syncrpc.NewHandler(rmqHandler, zap.L(), handlerProps)
	if err != nil {
		panic(err)
	}

	data, err := handler.ExecuteRequest(context.Background(), outgoingData{MagicNumber: 199})
	if err != nil {
		panic(err)
	}

	var result incomingData
	err = json.Unmarshal(data, &result)
	if err != nil {
		panic(err)
	}

	zap.L().Info("Got response", zap.String("response", result.ResponseText))
}

func getHandler() *rmqworker.RMQHandler {
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
		Logger:                  zap.L(),
	}

	h, err := rmqworker.NewRMQHandler(task)
	if err != nil {
		panic(err)
	}

	return h
}
