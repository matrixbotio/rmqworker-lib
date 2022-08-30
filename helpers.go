package rmqworker

import (
	"strings"

	"github.com/matrixbotio/constants-lib"

	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
)

func encodeMessage(message interface{}) ([]byte, errs.APIError) {
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
