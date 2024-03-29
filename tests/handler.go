package tests

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/matrixbotio/rmqworker-lib"
)

func GetHandler(t *testing.T) *rmqworker.RMQHandler {
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
		Logger:                  zaptest.NewLogger(t, zaptest.Level(zap.InfoLevel)),
	}

	h, err := rmqworker.NewRMQHandler(task)
	if err != nil {
		t.FailNow()
	}

	return h
}
