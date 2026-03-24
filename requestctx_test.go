package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGenerateRequestID(t *testing.T) {
	t.Run("non-empty", func(t *testing.T) {
		id := generateRequestID()
		if id == "" {
			t.Error("expected non-empty request ID")
		}
	})

	t.Run("hex format 16 chars", func(t *testing.T) {
		id := generateRequestID()
		if len(id) != 16 {
			t.Errorf("expected 16-char hex string, got %d chars: %q", len(id), id)
		}
		for _, c := range id {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
				t.Errorf("non-hex character %c in request ID %q", c, id)
			}
		}
	})

	t.Run("unique across calls", func(t *testing.T) {
		seen := make(map[string]bool)
		for i := 0; i < 100; i++ {
			id := generateRequestID()
			if seen[id] {
				t.Fatalf("duplicate request ID on iteration %d: %q", i, id)
			}
			seen[id] = true
		}
	})
}

func TestClientIPFrom(t *testing.T) {
	t.Run("X-Forwarded-For single", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/mcp", nil)
		r.Header.Set("X-Forwarded-For", "203.0.113.50")
		if got := clientIPFrom(r); got != "203.0.113.50" {
			t.Errorf("got %q, want 203.0.113.50", got)
		}
	})

	t.Run("X-Forwarded-For chain takes first", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/mcp", nil)
		r.Header.Set("X-Forwarded-For", "203.0.113.50, 10.0.0.1, 172.16.0.1")
		if got := clientIPFrom(r); got != "203.0.113.50" {
			t.Errorf("got %q, want 203.0.113.50", got)
		}
	})

	t.Run("X-Real-IP fallback", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/mcp", nil)
		r.Header.Set("X-Real-IP", "198.51.100.10")
		if got := clientIPFrom(r); got != "198.51.100.10" {
			t.Errorf("got %q, want 198.51.100.10", got)
		}
	})

	t.Run("RemoteAddr fallback strips port", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/mcp", nil)
		r.RemoteAddr = "192.168.1.100:54321"
		if got := clientIPFrom(r); got != "192.168.1.100" {
			t.Errorf("got %q, want 192.168.1.100", got)
		}
	})

	t.Run("X-Forwarded-For takes priority over X-Real-IP", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/mcp", nil)
		r.Header.Set("X-Forwarded-For", "203.0.113.50")
		r.Header.Set("X-Real-IP", "198.51.100.10")
		if got := clientIPFrom(r); got != "203.0.113.50" {
			t.Errorf("got %q, want 203.0.113.50", got)
		}
	})
}

func TestRequestCtxWrap(t *testing.T) {
	t.Run("sets X-Request-ID response header", func(t *testing.T) {
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		handler := requestCtxWrap(inner)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/mcp", nil)
		handler.ServeHTTP(rec, req)

		if got := rec.Header().Get("X-Request-ID"); got == "" {
			t.Error("expected X-Request-ID response header")
		}
	})

	t.Run("honors incoming X-Request-ID", func(t *testing.T) {
		var gotID string
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotID, _ = r.Context().Value(ctxKeyRequestID).(string)
		})
		handler := requestCtxWrap(inner)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/mcp", nil)
		req.Header.Set("X-Request-ID", "my-trace-123")
		handler.ServeHTTP(rec, req)

		if gotID != "my-trace-123" {
			t.Errorf("got %q, want my-trace-123", gotID)
		}
		if rec.Header().Get("X-Request-ID") != "my-trace-123" {
			t.Error("response header should echo the incoming X-Request-ID")
		}
	})

	t.Run("generates ID when no header", func(t *testing.T) {
		var gotID string
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotID, _ = r.Context().Value(ctxKeyRequestID).(string)
		})
		handler := requestCtxWrap(inner)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/mcp", nil)
		handler.ServeHTTP(rec, req)

		if gotID == "" {
			t.Error("expected generated request ID in context")
		}
	})

	t.Run("propagates client IP", func(t *testing.T) {
		var gotIP string
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotIP, _ = r.Context().Value(ctxKeyClientIP).(string)
		})
		handler := requestCtxWrap(inner)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/mcp", nil)
		req.Header.Set("X-Forwarded-For", "10.42.0.58")
		handler.ServeHTTP(rec, req)

		if gotIP != "10.42.0.58" {
			t.Errorf("got %q, want 10.42.0.58", gotIP)
		}
	})

	t.Run("propagates User-Agent", func(t *testing.T) {
		var gotUA string
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotUA, _ = r.Context().Value(ctxKeyUserAgent).(string)
		})
		handler := requestCtxWrap(inner)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/mcp", nil)
		req.Header.Set("User-Agent", "claude-code/1.0")
		handler.ServeHTTP(rec, req)

		if gotUA != "claude-code/1.0" {
			t.Errorf("got %q, want claude-code/1.0", gotUA)
		}
	})
}

func TestRequestAttrs(t *testing.T) {
	t.Run("nil for empty context", func(t *testing.T) {
		attrs := requestAttrs(context.Background())
		if attrs != nil {
			t.Errorf("expected nil attrs for empty context, got %v", attrs)
		}
	})

	t.Run("returns attrs for populated context", func(t *testing.T) {
		ctx := context.Background()
		ctx = context.WithValue(ctx, ctxKeyRequestID, "abc123")
		ctx = context.WithValue(ctx, ctxKeyClientIP, "10.0.0.1")
		ctx = context.WithValue(ctx, ctxKeyUserAgent, "test-agent")

		attrs := requestAttrs(ctx)
		if len(attrs) != 3 {
			t.Fatalf("expected 3 attrs, got %d", len(attrs))
		}
		// req, ip, ua
		if attrs[0].Key != "req" || attrs[0].Value.String() != "abc123" {
			t.Errorf("unexpected req attr: %v", attrs[0])
		}
		if attrs[1].Key != "ip" || attrs[1].Value.String() != "10.0.0.1" {
			t.Errorf("unexpected ip attr: %v", attrs[1])
		}
		if attrs[2].Key != "ua" || attrs[2].Value.String() != "test-agent" {
			t.Errorf("unexpected ua attr: %v", attrs[2])
		}
	})

	t.Run("omits empty ip and ua", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), ctxKeyRequestID, "abc123")
		attrs := requestAttrs(ctx)
		if len(attrs) != 1 {
			t.Fatalf("expected 1 attr (req only), got %d", len(attrs))
		}
	})
}

func TestLoggingWithRequestContext(t *testing.T) {
	t.Run("includes req ID in info log", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelInfo)
		wrapped := withLogging(logger, "read_graph", successHandler(`{"totalEntities":5}`))

		ctx := context.WithValue(context.Background(), ctxKeyRequestID, "trace-42")
		ctx = context.WithValue(ctx, ctxKeyClientIP, "10.0.0.1")
		wrapped(ctx, makeRequest(map[string]any{"mode": "summary"}))

		output := buf.String()
		if !strings.Contains(output, "req=trace-42") {
			t.Errorf("expected req=trace-42 in log: %s", output)
		}
		if !strings.Contains(output, "ip=10.0.0.1") {
			t.Errorf("expected ip=10.0.0.1 in log: %s", output)
		}
	})

	t.Run("includes req ID in error log", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelInfo)
		wrapped := withLogging(logger, "open_nodes", errorHandler("not found"))

		ctx := context.WithValue(context.Background(), ctxKeyRequestID, "trace-99")
		wrapped(ctx, makeRequest(nil))

		output := buf.String()
		if !strings.Contains(output, "req=trace-99") {
			t.Errorf("expected req=trace-99 in error log: %s", output)
		}
	})

	t.Run("includes raw_args in error log", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelInfo)
		wrapped := withLogging(logger, "open_nodes", errorHandler("invalid arguments: bad"))

		wrapped(context.Background(), makeRequest(map[string]any{"names": "Scout"}))

		output := buf.String()
		if !strings.Contains(output, "raw_args=") {
			t.Errorf("expected raw_args in error log: %s", output)
		}
		if !strings.Contains(output, "Scout") {
			t.Errorf("expected Scout in raw_args: %s", output)
		}
	})

	t.Run("no req field for stdio context", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelInfo)
		wrapped := withLogging(logger, "read_graph", successHandler(`{"totalEntities":1}`))
		wrapped(context.Background(), makeRequest(map[string]any{"mode": "summary"}))

		if strings.Contains(buf.String(), "req=") {
			t.Errorf("should not have req= for bare context: %s", buf.String())
		}
	})
}
