package rmqworker

import "errors"

var ErrCSTXTimeout = errors.New("crossServiceTransaction timeout")

func ConvertAPIError(err APIError) error {
	if err != nil {
		return *err
	}
	return nil
}
