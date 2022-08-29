package cstx

import (
	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
)

type handler interface {
	PublishCSXTToQueue(task structs.RMQPublishRequestTask, cstx CrossServiceTransaction) errs.APIError
	PublishCSXTToExchange(task structs.PublishToExchangeTask, cstx CrossServiceTransaction) errs.APIError

	PublishToExchange(task structs.PublishToExchangeTask, additionalHeaders ...structs.RMQHeader) errs.APIError
}
