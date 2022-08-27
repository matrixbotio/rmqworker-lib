package rmqworker

func ConvertAPIError(err APIError) error {
	if err != nil {
		return *err
	}
	return nil
}
