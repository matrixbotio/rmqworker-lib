package syncrpc

import (
	"time"
)

const (
	testTimeout = 5 * time.Second

	requestsExchange  = "some_external_service_that_does_some_work"
	responsesExchange = "some_external_service_that_does_some_work.response"
)
