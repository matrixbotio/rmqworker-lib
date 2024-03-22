package rmqworker

import (
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/matrixbotio/constants-lib"
)

const (
	handlerFirstConnTimeout              = 10 * time.Second
	waitBetweenReconnect                 = 5 * time.Second
	publishTimeout                       = 2 * time.Second
	defaultContentType                   = "application/json"
	requestHandlerDefaultMessageLifetime = 5 * 60 * 1000 // ms
)

var (
	publishDeadlineError = constants.Error(
		"SERVICE_REQ_TIMEOUT", "message publishing timeout",
	)
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

const (
	ExchangeTypeDirect = "direct"
	ExchangeTypeTopic  = "topic"
	ExchangeTypeFanout = "fanout"
)
