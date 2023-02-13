package dependencies

import "github.com/matrixbotio/rmqworker-lib/pkg/errs"

type RMQDeliveryHandler interface {
	GetResponseRoutingKeyHeader() (string, errs.APIError)
	GetCorrelationID() string
}
