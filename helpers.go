package rmqworker

import (
	"encoding/json"

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

func convertRMQError(err APIError) *constants.APIError {
	return (*constants.APIError)(err)
}
