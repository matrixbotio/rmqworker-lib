package rmqworker

import (
	"time"

	"github.com/matrixbotio/constants-lib"
)

const (
	waitBetweenReconnect = 5 * time.Second
	//deliveryCallbackTimeout = 90 * time.Second
	publishMaxAttempts  = 3
	publishAttemptDelay = 50 * time.Millisecond
	publishTimeout      = 2 * time.Second
)

var (
	publishDeadlineError = constants.Error(
		"SERVICE_REQ_TIMEOUT", "message publishing timeout",
	)
)
