package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/cmd"
)

func main() {
	h := cmd.GetHandler()

	workerTask := rmqworker.WorkerTask{
		QueueName:      "alex",
		RoutingKey:     "alex",
		ISQueueDurable: false,
		ISAutoDelete:   false,
		Callback: func(w *rmqworker.RMQWorker, deliveryHandler rmqworker.RMQDeliveryHandler) {
			log.Printf("received: %s", deliveryHandler.GetMessageBody())
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
		log.Printf("Finishing")
		worker.Finish()
	}()

	worker.AwaitFinish()
}
