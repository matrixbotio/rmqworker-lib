package cstx_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/matrixbotio/rmqworker-lib/pkg/cstx"
)

func Test_GetCstx_Found(t *testing.T) {
	tx := cstx.New(1, 1, cstx.NewMockHandler(t))
	ctx := cstx.WithCstx(context.Background(), tx)

	res, ok := cstx.GetCstx(ctx)
	assert.Equal(t, tx, res)
	assert.True(t, ok)
}

func Test_GetCstx_NotFound(t *testing.T) {
	res, ok := cstx.GetCstx(context.Background())
	assert.Nil(t, res)
	assert.False(t, ok)
}

func Test_GetCstx_Nil(t *testing.T) {
	var tx cstx.CSTX
	ctx := cstx.WithCstx(context.Background(), tx)

	res, ok := cstx.GetCstx(ctx)
	assert.Nil(t, res)
	assert.False(t, ok)
}
