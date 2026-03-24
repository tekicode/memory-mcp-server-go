package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
)

type ctxKey int

const (
	ctxKeyRequestID ctxKey = iota
	ctxKeyClientIP
	ctxKeyUserAgent
)

// requestCtxWrap injects request tracing metadata into the context:
// request ID (from X-Request-ID header or generated), client IP, and User-Agent.
func requestCtxWrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if !validRequestID(reqID) {
			reqID = generateRequestID()
		}
		w.Header().Set("X-Request-ID", reqID)

		ctx := r.Context()
		ctx = context.WithValue(ctx, ctxKeyRequestID, reqID)
		ctx = context.WithValue(ctx, ctxKeyClientIP, clientIPFrom(r))
		ctx = context.WithValue(ctx, ctxKeyUserAgent, r.Header.Get("User-Agent"))

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// maxRequestIDLen is the maximum length accepted for an incoming X-Request-ID.
const maxRequestIDLen = 128

// validRequestID checks that an incoming request ID is non-empty, within the
// length limit, and contains only printable ASCII (no control chars or newlines).
func validRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLen {
		return false
	}
	for _, c := range id {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}

// generateRequestID produces a random 16-char hex string.
// On Go 1.22+ crypto/rand.Read always returns len(p), nil (panics on failure).
func generateRequestID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}

// clientIPFrom extracts the client IP from the request, checking
// X-Forwarded-For, X-Real-IP, then RemoteAddr.
func clientIPFrom(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i != -1 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	if xri := strings.TrimSpace(r.Header.Get("X-Real-IP")); xri != "" {
		return xri
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// maxAttrLen caps logged header-sourced values to prevent log bloat.
const maxAttrLen = 64

// requestAttrs extracts request tracing metadata from context as slog attributes.
// Returns nil if no request metadata is present (e.g., stdio transport).
func requestAttrs(ctx context.Context) []slog.Attr {
	reqID, _ := ctx.Value(ctxKeyRequestID).(string)
	if reqID == "" {
		return nil
	}
	attrs := []slog.Attr{slog.String("req", reqID)}
	if ip, _ := ctx.Value(ctxKeyClientIP).(string); ip != "" {
		attrs = append(attrs, slog.String("ip", truncate(ip, maxAttrLen)))
	}
	if ua, _ := ctx.Value(ctxKeyUserAgent).(string); ua != "" {
		attrs = append(attrs, slog.String("ua", truncate(ua, maxAttrLen)))
	}
	return attrs
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
