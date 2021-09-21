package rmqworker

import "github.com/matrixbotio/constants-lib"

type apiError *constants.APIError

type rmqConnectionData struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     string `json:"port"`
}
