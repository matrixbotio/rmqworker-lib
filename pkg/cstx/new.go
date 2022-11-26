package cstx

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

func New(ackNum, timeout int32, handler Handler) CrossServiceTransaction {
	return CrossServiceTransaction{
		Handler:   handler,
		ID:        uuid.NewString(),
		AckNum:    ackNum,
		StartedAt: time.Now().UnixMilli(),
		Timeout:   timeout,
	}
}

func NewFromJson(data string, handler Handler) (CrossServiceTransaction, error) {
	var tx CrossServiceTransaction
	if err := json.Unmarshal([]byte(data), &tx); err != nil {
		return tx, err
	}
	tx.Handler = handler
	return tx, nil
}
