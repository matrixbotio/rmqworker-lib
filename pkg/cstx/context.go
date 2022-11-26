package cstx

import "context"

type contextKey string

const ctxKey contextKey = "cstx"

func WithCstx(ctx context.Context, cstx CSTX) context.Context {
	return context.WithValue(ctx, ctxKey, cstx)
}

func GetCstx(ctx context.Context) (CSTX, bool) {
	val, ok := ctx.Value(ctxKey).(CSTX)
	return val, ok
}
