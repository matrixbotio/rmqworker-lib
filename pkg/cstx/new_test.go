package cstx_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/matrixbotio/rmqworker-lib/pkg/cstx"
)

func Test_NewFromJson(t *testing.T) {
	const (
		ackNum    = int32(55)
		timeout   = int32(77)
		startedAt = int64(999)
		id        = "cstxid"
	)
	handler := cstx.NewMockHandler(t)

	tx := cstx.New(ackNum, timeout, handler)
	tx.ID = id
	tx.StartedAt = startedAt
	serialized := tx.Serialize()

	restored, err := cstx.NewFromJson(serialized, handler)
	assert.NoError(t, err)
	assert.Equal(t, ackNum, restored.AckNum)
	assert.Equal(t, timeout, restored.Timeout)
	assert.Equal(t, startedAt, restored.StartedAt)
	assert.Equal(t, id, restored.ID)
	assert.Same(t, handler, restored.Handler)
}
