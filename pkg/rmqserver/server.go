package rmqserver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/matrixbotio/rmqworker-lib"
	"go.uber.org/zap"
)

type Server struct {
	rmqHandler *rmqworker.RMQHandler
	handlers   *map[string]HandlerFunc
}

type HandlerFunc func(context.Context, *json.RawMessage) (any, error)

func NewServer(
	rmqHandler *rmqworker.RMQHandler,
	task *rmqworker.WorkerTask,
	handlers *map[string]HandlerFunc,
) (*Server, error) {
	s := &Server{
		rmqHandler: rmqHandler,
		handlers:   handlers,
	}

	task.Callback = s.callback
	task.UseErrorCallback = true
	task.ErrorCallback = s.errorCallback

	w, apiErr := s.rmqHandler.NewRMQWorker(*task)
	if apiErr != nil {
		return nil, fmt.Errorf("rmqserver.NewServer create rmqWorker: %w", *apiErr)
	}

	if apiErr = w.Serve(); apiErr != nil {
		return nil, fmt.Errorf("rmqserver.NewServer serve rmqWorker: %w", *apiErr)
	}

	zap.L().Info(task.QueueName + " queue start listening...")

	return s, nil
}

func (s *Server) manager(method string, rawData *json.RawMessage) (any, error) {
	if handler, ok := (*s.handlers)[method]; ok {
		res, err := handler(context.Background(), rawData)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	return nil, fmt.Errorf("rmqserver.manager unknown method: %s", method)
}
