package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/matrixbotio/constants-lib"
	"github.com/matrixbotio/go-common-lib/zes"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
)

const (
	requestsExchange  = "some_external_service_that_does_some_work"
	responsesExchange = "some_external_service_that_does_some_work.response"
)

type incomingData struct {
	MagicNumber int
}

type outgoingData struct {
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
	runWorker(rmqHandler)

	zap.L().Info("Started")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
	zap.L().Info("Stopping application...")
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

func runWorker(rmqHandler *rmqworker.RMQHandler) {
	w, apiErr := rmqHandler.NewRMQWorker(rmqworker.WorkerTask{
		QueueName:  requestsExchange,
		RoutingKey: requestsExchange + "-r-key",

		ISQueueDurable: true,
		ISAutoDelete:   false,
		Callback: func(_ *rmqworker.RMQWorker, deliveryHandler rmqworker.RMQDeliveryHandler) {
			if apiErr := deliveryHandler.CheckResponseError(); apiErr != nil {
				panic(apiErr)
			}

			var dataFromClient incomingData
			if err := json.Unmarshal(deliveryHandler.GetMessageBody(), &dataFromClient); err != nil {
				panic(err)
			}

			zap.L().Info("incoming data", zap.String("data", fmt.Sprintf("%+v", dataFromClient)))

			responseRoutingKeyHeader, apiErr := deliveryHandler.GetResponseRoutingKeyHeader()
			if apiErr != nil {
				panic(apiErr)
			}

			zap.L().Info("responseRoutingKeyHeader", zap.String("value", responseRoutingKeyHeader))

			apiErr = rmqHandler.PublishToExchange(structs.PublishToExchangeTask{
				CorrelationID: deliveryHandler.GetCorrelationID(),
				Message: outgoingData{
					ResponseText: "everything is okay: " + strconv.Itoa(dataFromClient.MagicNumber),
				},
				ExchangeName: responsesExchange,
				RoutingKey:   responseRoutingKeyHeader,
			})
			if apiErr != nil {
				panic(apiErr)
			}

			zap.L().Info("answer sent successfully")
		},
		FromExchange:     requestsExchange,
		ExchangeType:     "topic",
		ConsumersCount:   1,
		MessagesLifetime: int64(1 * 60 * 1000),
		UseErrorCallback: true,
		ErrorCallback: func(*rmqworker.RMQWorker, *constants.APIError) {
			panic("should not happened")
		},
		DisableCheckResponseErrors: true,
	})

	if apiErr != nil {
		panic(apiErr)
	}

	apiErr = w.Serve()
	if apiErr != nil {
		panic(apiErr)
	}
}
