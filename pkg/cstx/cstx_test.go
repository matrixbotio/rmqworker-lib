package cstx_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/matrixbotio/rmqworker-lib/pkg/cstx"
)

func Test_CstxSerialize(t *testing.T) {
	tx := cstx.New(1, 1, cstx.NewMockHandler(t))
	res := tx.Serialize()
	assert.NotEmpty(t, res)
}
