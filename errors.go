package rmqworker

import (
	"errors"

	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
)

var ErrCSTXTimeout = errors.New("crossServiceTransaction timeout")

func ConvertAPIError(err errs.APIError) error {
	if err != nil {
		return *err
	}
	return nil
}
