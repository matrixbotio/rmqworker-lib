package utils

import (
	"encoding/json"
	"fmt"
)

func GetRequest[T any](deliveryHandler RMQDeliveryHandler) (T, error) {
	bodyBytes := deliveryHandler.GetMessageBody()

	request := new(T)

	if len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, &request); err != nil {
			return *request, fmt.Errorf("unmarshal request: %w", err)
		}
	}

	return *request, nil
}
