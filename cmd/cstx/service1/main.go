package main

//  {"data": "alex"}

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/cmd"
)

const queue = "service1"

func main() {
	h := cmd.GetHandler()

	workerTask := rmqworker.WorkerTask{
		QueueName:      queue,
		RoutingKey:     queue,
		ISQueueDurable: false,
		ISAutoDelete:   false,
		Callback: func(w *rmqworker.RMQWorker, deliveryHandler rmqworker.RMQDeliveryHandler) {
			transaction := deliveryHandler.GetCSTX(h)

			log.Printf(
				"cstx ID: %s\nreceived data: %s\n",
				transaction.ID,
				deliveryHandler.GetMessageBody(),
			)

			res, err := transaction.Commit()
			if err != nil {
				log.Printf("commit error: %#v", err)
			}
			log.Printf("commit result: %v", res)
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