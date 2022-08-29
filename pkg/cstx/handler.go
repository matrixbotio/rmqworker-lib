package cstx

import (
	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
	"github.com/matrixbotio/rmqworker-lib/pkg/tasks"
)

type handler interface {
	PublishToQueue(task tasks.RMQPublishRequestTask) errs.APIError
	PublishToExchange(task tasks.PublishToExchangeTask) errs.APIError
}
