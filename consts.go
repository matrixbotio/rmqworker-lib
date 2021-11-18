package rmqworker

const (
	reconnectionAttemptsNumber    = 3
	reconnectAfterSeconds         = 2
	reconnectionTimeout           = 4  // seconds
	waitingBetweenAttempts        = 60 // seconds
	waitingBetweenMsgSubscription = 5  // seconds
	cronConnectionCheckTimeout    = 3  // minutes
)
