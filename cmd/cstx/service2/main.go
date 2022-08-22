package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/matrixbotio/go-common-lib/zes"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/cmd"
)

const queue = "service2"

func main() {
	logger, loggerErr := zes.Init(false)
	if loggerErr != nil {
		panic(loggerErr)
	}
	defer logger.Close()
	zap.ReplaceGlobals(logger.New(zap.DebugLevel))

	h := cmd.GetHandler()

	h.StartCSTXAcksConsumer()

	workerTask := rmqworker.WorkerTask{
		QueueName:      queue,
		RoutingKey:     queue,
		ISQueueDurable: false,
		ISAutoDelete:   false,
		Callback: func(w *rmqworker.RMQWorker, deliveryHandler rmqworker.RMQDeliveryHandler) {
			transaction := deliveryHandler.GetCSTX(h)

			zap.L().Debug(fmt.Sprintf(
				"cstx ID: %s; received data: %s",
				transaction.ID,
				deliveryHandler.GetMessageBody(),
			))

			res, err := transaction.Commit()
			if err != nil {
				zap.L().Error(fmt.Sprintf("commit error: %#v", err))
			} else {
				zap.L().Debug(fmt.Sprintf("commit result: %v", res))
			}
		},
	}
	worker, err := h.NewRMQWorker(workerTask)
	if err != nil {
		panic(err)
	}

	err = worker.Serve()
	if err != nil {
		panic(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		worker.Finish()
	}()

	worker.AwaitFinish()
}
