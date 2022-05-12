package rmqworker

import (
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/matrixbotio/constants-lib"
)

const (
	waitBetweenReconnect = 5 * time.Second
	//deliveryCallbackTimeout = 90 * time.Second
	publishMaxAttempts                   = 3
	publishAttemptDelay                  = 50 * time.Millisecond
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
