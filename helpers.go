package rmqworker

import (
	"strings"

	"github.com/google/uuid"
	"github.com/matrixbotio/constants-lib"
)

func getUUID() string {
	return uuid.New().String()
}

func encodeMessage(message interface{}) ([]byte, APIError) {
	jsonBytes, marshalErr := json.Marshal(message)
	if marshalErr != nil {
		return nil, constants.Error(
			"DATA_ENCODE_ERR",
			"failed to marshal message to json: "+marshalErr.Error(),
		)
	}
	return jsonBytes, nil
}

func getRMQConnectionURL(connData RMQConnectionData) string {
	var useTLS bool = false
	if connData.UseTLS {
		useTLS = true
	}

	protocol := "amqp"
	if useTLS {
		protocol += "s"
	}
	return protocol + "://" + connData.User + ":" + connData.Password +
		"@" + connData.Host + ":" + connData.Port + "/"
}

func checkContextDeadlineErr(err error) bool {
	return strings.Contains(err.Error(), "context deadline")
}

func ternary(statement bool, a, b string) string {
	if statement {
		return a
	}
	return b
}
