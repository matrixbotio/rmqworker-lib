package rmqworker

const (
	reconnectionAttemptsNumber = 3
	reconnectAfterSeconds      = 2
	reconnectionTimeout        = 4  // seconds
	waitingBetweenAttempts     = 60 // seconds
	cronConnectionCheckTimeout = 3  // minutes
)
