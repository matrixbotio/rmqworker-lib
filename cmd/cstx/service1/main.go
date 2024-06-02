package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/cmd"
)

const queue = "service1"

func main() {
	logger, loggerErr := zap.NewDevelopment()
	if loggerErr != nil {
		panic(loggerErr)
	}
	defer logger.Sync()
	zap.ReplaceGlobals(logger)

	h := cmd.GetHandler()

	h.StartCSTXAcksConsumer()

	workerTask := rmqworker.WorkerTask{
		QueueName:      queue,
		RoutingKey:     queue,
		ISQueueDurable: false,
		ISAutoDelete:   false,
		Callback: func(w *rmqworker.RMQWorker, deliveryHandler rmqworker.RMQDeliveryHandler) {
			transaction := deliveryHandler.GetCSTX(h)
			if transaction.ID == "" {
				return
			}

			zap.L().Info(
				"get transaction from rabbit-queue",
				zap.String("struct", fmt.Sprintf("%#v", transaction)),
				zap.String("message-body", string(deliveryHandler.GetMessageBody())),
			)

			if err := transaction.Commit(); err != nil {
				zap.L().Info("commit error", zap.Error(err))
				return
			}

			zap.L().Info("commit success")
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
