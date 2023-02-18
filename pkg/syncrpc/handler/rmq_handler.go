package handler

import (
	"github.com/matrixbotio/constants-lib"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
)

type RMQHandler interface {
	SendRMQResponse(task *rmqworker.RMQPublishResponseTask, errorMsg ...*constants.APIError) errs.APIError
	NewRMQWorker(task rmqworker.WorkerTask) (*rmqworker.RMQWorker, errs.APIError)
}
