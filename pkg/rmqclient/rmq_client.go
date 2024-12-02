package rmqclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/matrixbotio/constants-lib"
	"github.com/matrixbotio/go-common-lib/pkg/nano"
	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
)

// RmqSync is a wrapper for RMQHandler that allows to send request to exchange and wait for response
// WorkerTask.QueueName is a name of queue where responses will listen

const requestTTL = time.Second * 30
const reconnectTTL = time.Second * 5

type RmqClient struct {
	rmqHandler    *rmqworker.RMQHandler
	responses     sync.Map
	consumerQueue string
	requestQueue  string
}

type response struct {
	data  []byte
	error error
}

type Message struct {
	Method string `json:"method"`
	Data   any    `json:"data"`
}

func New(rmqHandler *rmqworker.RMQHandler, t *rmqworker.WorkerTask, requestQueue string) (*RmqClient, error) {

	c := &RmqClient{
		rmqHandler:    rmqHandler,
		consumerQueue: t.QueueName,
		requestQueue:  requestQueue,
	}

	t.Callback = c.callback
	t.UseErrorCallback = true
	t.ErrorCallback = c.errorCallback

	w, apiErr := c.rmqHandler.NewRMQWorker(*t)
	if apiErr != nil {
		return nil, fmt.Errorf("rmqclient.New create rmqWorker: %w", *apiErr)
	}

	if apiErr = w.Serve(); apiErr != nil {
		return nil, fmt.Errorf("rmqclient.New serve rmqWorker: %w", *apiErr)
	}

	zap.L().Info(t.QueueName + " queue start listening...")

	return c, nil
}

// Publish request to exchange and async wait for response
// if t.ResponseRoutingKey is empty, no need to wait for response
func (c *RmqClient) Publish(ctx context.Context, t *structs.RMQPublishRequestTask) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTTL)
	defer cancel()

	t.CorrelationID = nano.ID()

	responseCh := make(chan response)
	c.responses.Store(t.CorrelationID, responseCh)
	defer c.responses.Delete(t.CorrelationID)

	if apiErr := c.rmqHandler.PublishToQueue(*t); apiErr != nil {
		return nil, fmt.Errorf("rmqclient.Publish publish request to exchange: %w", *apiErr)
	}

	if t.ResponseRoutingKey == "" {
		return nil, nil
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context: %w", ctx.Err())
	case response := <-responseCh:
		if response.error != nil {
			return nil, response.error
		}
		return response.data, nil
	}
}

// shorthand for Publish
func (c *RmqClient) Send(ctx context.Context, message *Message) ([]byte, error) {
	t := &structs.RMQPublishRequestTask{
		QueueName:          c.requestQueue,
		MessageBody:        message,
		ResponseRoutingKey: c.consumerQueue,
	}

	return c.Publish(ctx, t)
}

// Handle response from exchange
func (c *RmqClient) callback(_ *rmqworker.RMQWorker, deliveryHandler rmqworker.RMQDeliveryHandler) {
	var responseCh chan response
	if ch, found := c.responses.Load(deliveryHandler.GetCorrelationID()); !found {
		return
	} else {
		responseCh = ch.(chan response)
	}

	response := response{
		data: deliveryHandler.GetMessageBody(),
	}
	if apiErr := deliveryHandler.CheckResponseError(); apiErr != nil {
		response.error = errors.New(apiErr.Message)
	}

	responseCh <- response
	close(responseCh)
}

// Handle error from exchange
func (c *RmqClient) errorCallback(worker *rmqworker.RMQWorker, err *constants.APIError) {
	if err != nil {
		zap.L().Error(
			"rmqclient.errorCallback",
			zap.Error(errors.New(err.Message)),
			zap.String("queue", c.consumerQueue),
		)
	}

	var res response
	c.responses.Range(func(key, value any) bool {
		res.error = errors.New(err.Message)
		responseCh := value.(chan response)

		responseCh <- res
		close(responseCh)

		return true
	})

	go c.reconnect(worker)
}

// Reconnect to RMQ
func (c *RmqClient) reconnect(worker *rmqworker.RMQWorker) {
	for {
		zap.L().Info("Attempting to reconnect to RMQ...")
		apiErr := worker.Serve()
		if apiErr == nil {
			zap.L().Info("Reconnected to RMQ successfully.")
			return
		}

		zap.L().Error(
			"Failed to reconnect to RMQ, retrying...",
			zap.Error(errors.New(apiErr.Message)),
		)

		time.Sleep(reconnectTTL)
	}
}
