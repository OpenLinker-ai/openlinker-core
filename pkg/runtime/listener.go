package runtime

import "context"

type mtlsListenerContextKey struct{}

// WithMTLSListener is for the server's dedicated, verifying TLS listener only.
// It cannot be supplied by a request header.
func WithMTLSListener(ctx context.Context) context.Context {
	return context.WithValue(ctx, mtlsListenerContextKey{}, true)
}

func FromMTLSListener(ctx context.Context) bool {
	value, _ := ctx.Value(mtlsListenerContextKey{}).(bool)
	return value
}
