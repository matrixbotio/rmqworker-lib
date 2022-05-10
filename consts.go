package rmqworker

import "time"

const (
	waitBetweenReconnect    = 5 * time.Second
	deliveryCallbackTimeout = 90 // seconds

	baseInternalError = "BASE_INTERNAL_ERROR"
)
