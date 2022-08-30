package tests

import (
	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
)

func ConvertAPIError(err errs.APIError) error {
	if err != nil {
		return *err
	}
	return nil
}
