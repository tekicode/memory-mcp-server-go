package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

// allToolNames is the canonical list of all 13 tool names.
var allToolNames = []string{
	"create_entities",
	"create_relations",
	"add_observations",
	"delete_entities",
	"delete_observations",
	"delete_relations",
	"read_graph",
	"search_nodes",
	"open_nodes",
	"merge_entities",
	"update_entities",
	"update_observations",
	"detect_conflicts",
}

// testLogger creates a logger writing to a buffer at the given level.
func testLogger(level slog.Level) (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: level}))
	return logger, &buf
}

// makeRequest creates a CallToolRequest with the given args map.
func makeRequest(args map[string]any) mcp.CallToolRequest {
	return mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: args,
		},
	}
}

// successHandler returns a handler that always succeeds with the given text.
func successHandler(text string) ToolHandler {
	return func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText(text), nil
	}
}

// errorHandler returns a handler that always fails with the given error.
func errorHandler(errMsg string) ToolHandler {
	return func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return nil, errors.New(errMsg)
	}
}

// --- AC-1.2: Passthrough ---

func TestPassthrough(t *testing.T) {
	t.Run("result unmodified", func(t *testing.T) {
		logger, _ := testLogger(slog.LevelInfo)
		expected := mcp.NewToolResultText(`[{"name":"Scout"}]`)

		handler := func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return expected, nil
		}

		wrapped := withLogging(logger, "create_entities", handler)
		got, err := wrapped(context.Background(), makeRequest(nil))

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != expected {
			t.Error("result pointer was modified")
		}
	})

	t.Run("error unmodified", func(t *testing.T) {
		logger, _ := testLogger(slog.LevelInfo)
		expectedErr := errors.New("entity not found")

		handler := func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return nil, expectedErr
		}

		wrapped := withLogging(logger, "open_nodes", handler)
		result, err := wrapped(context.Background(), makeRequest(nil))

		if err != expectedErr {
			t.Errorf("error was modified: got %v, want %v", err, expectedErr)
		}
		if result != nil {
			t.Error("result should be nil when handler returns nil")
		}
	})

	t.Run("both result and error passed through", func(t *testing.T) {
		logger, _ := testLogger(slog.LevelInfo)
		expectedResult := mcp.NewToolResultText("partial")
		expectedErr := errors.New("partial failure")

		handler := func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return expectedResult, expectedErr
		}

		wrapped := withLogging(logger, "search_nodes", handler)
		got, err := wrapped(context.Background(), makeRequest(nil))

		if got != expectedResult {
			t.Error("result pointer was modified")
		}
		if err != expectedErr {
			t.Error("error was modified")
		}
	})
}

// --- AC-1.3: InfoLog ---

func TestInfoLog(t *testing.T) {
	t.Run("includes tool name as msg", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelInfo)
		wrapped := withLogging(logger, "open_nodes", successHandler(`{"entities":[],"relations":[]}`))
		wrapped(context.Background(), makeRequest(map[string]any{"names": []any{"Scout"}}))

		if !strings.Contains(buf.String(), "msg=open_nodes") {
			t.Errorf("expected msg=open_nodes in log output: %s", buf.String())
		}
	})

	t.Run("includes key params", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelInfo)
		wrapped := withLogging(logger, "search_nodes", successHandler(`{"entities":[],"total":0,"limit":0,"hasMore":false}`))
		wrapped(context.Background(), makeRequest(map[string]any{"query": "gender"}))

		output := buf.String()
		if !strings.Contains(output, "query=gender") {
			t.Errorf("expected query=gender in log: %s", output)
		}
	})

	t.Run("includes duration", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelInfo)
		wrapped := withLogging(logger, "read_graph", successHandler(`{"totalEntities":5}`))
		wrapped(context.Background(), makeRequest(map[string]any{"mode": "summary"}))

		if !strings.Contains(buf.String(), "duration=") {
			t.Errorf("expected duration= in log: %s", buf.String())
		}
	})

	t.Run("includes result summary", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelInfo)
		result := `{"entities":[{"name":"Scout","observations":["obs1","obs2"]},{"name":"Teki","observations":["obs3"]}],"relations":[]}`
		wrapped := withLogging(logger, "open_nodes", successHandler(result))
		wrapped(context.Background(), makeRequest(map[string]any{"names": []any{"Scout", "Teki"}}))

		output := buf.String()
		if !strings.Contains(output, "entities=2") {
			t.Errorf("expected entities=2 in log: %s", output)
		}
		if !strings.Contains(output, "observations=3") {
			t.Errorf("expected observations=3 in log: %s", output)
		}
	})

	t.Run("not logged at error level", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelError)
		wrapped := withLogging(logger, "read_graph", successHandler(`{"totalEntities":1}`))
		wrapped(context.Background(), makeRequest(nil))

		if buf.Len() > 0 {
			t.Errorf("INFO log should not appear at error level: %s", buf.String())
		}
	})
}

// --- AC-1.4: ErrorLog ---

func TestErrorLog(t *testing.T) {
	t.Run("logs at error level", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelInfo)
		wrapped := withLogging(logger, "open_nodes", errorHandler("entity not found: Scout"))
		wrapped(context.Background(), makeRequest(nil))

		output := buf.String()
		if !strings.Contains(output, "level=ERROR") {
			t.Errorf("expected level=ERROR in log: %s", output)
		}
	})

	t.Run("includes tool name", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelInfo)
		wrapped := withLogging(logger, "delete_entities", errorHandler("not found"))
		wrapped(context.Background(), makeRequest(nil))

		if !strings.Contains(buf.String(), "msg=delete_entities") {
			t.Errorf("expected msg=delete_entities in log: %s", buf.String())
		}
	})

	t.Run("includes duration", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelInfo)
		wrapped := withLogging(logger, "open_nodes", errorHandler("fail"))
		wrapped(context.Background(), makeRequest(nil))

		if !strings.Contains(buf.String(), "duration=") {
			t.Errorf("expected duration= in log: %s", buf.String())
		}
	})

	t.Run("includes error message", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelInfo)
		wrapped := withLogging(logger, "open_nodes", errorHandler("entity not found: Scout"))
		wrapped(context.Background(), makeRequest(nil))

		if !strings.Contains(buf.String(), "entity not found: Scout") {
			t.Errorf("expected error message in log: %s", buf.String())
		}
	})
}

// --- AC-1.5: DebugLog ---

func TestDebugLog(t *testing.T) {
	t.Run("logs full args JSON before handler", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelDebug)
		wrapped := withLogging(logger, "open_nodes", successHandler(`{"entities":[],"relations":[]}`))
		wrapped(context.Background(), makeRequest(map[string]any{"names": []any{"Scout", "Teki"}}))

		output := buf.String()
		if !strings.Contains(output, "open_nodes.request") {
			t.Errorf("expected open_nodes.request in debug log: %s", output)
		}
		if !strings.Contains(output, "Scout") || !strings.Contains(output, "Teki") {
			t.Errorf("expected args JSON with entity names in debug log: %s", output)
		}
	})

	t.Run("debug line appears before info line", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelDebug)
		wrapped := withLogging(logger, "search_nodes", successHandler(`{"entities":[],"total":0,"limit":0,"hasMore":false}`))
		wrapped(context.Background(), makeRequest(map[string]any{"query": "test"}))

		output := buf.String()
		debugIdx := strings.Index(output, "search_nodes.request")
		infoIdx := strings.Index(output, "msg=search_nodes ")
		if debugIdx < 0 || infoIdx < 0 {
			t.Fatalf("expected both debug and info lines, got: %s", output)
		}
		if debugIdx >= infoIdx {
			t.Error("debug line should appear before info line")
		}
	})

	t.Run("not logged at info level", func(t *testing.T) {
		logger, buf := testLogger(slog.LevelInfo)
		wrapped := withLogging(logger, "open_nodes", successHandler(`{"entities":[],"relations":[]}`))
		wrapped(context.Background(), makeRequest(map[string]any{"names": []any{"Scout"}}))

		if strings.Contains(buf.String(), ".request") {
			t.Errorf("debug log should not appear at info level: %s", buf.String())
		}
	})
}

// --- AC-1.1: LogLevel ---

func TestLogLevel(t *testing.T) {
	t.Run("valid levels", func(t *testing.T) {
		cases := []struct {
			input string
			want  slog.Level
		}{
			{"error", slog.LevelError},
			{"ERROR", slog.LevelError},
			{"info", slog.LevelInfo},
			{"Info", slog.LevelInfo},
			{"debug", slog.LevelDebug},
			{"DEBUG", slog.LevelDebug},
		}
		for _, tc := range cases {
			got, err := parseLogLevel(tc.input)
			if err != nil {
				t.Errorf("parseLogLevel(%q) unexpected error: %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("parseLogLevel(%q) = %v, want %v", tc.input, got, tc.want)
			}
		}
	})

	t.Run("invalid levels", func(t *testing.T) {
		for _, input := range []string{"warn", "trace", "verbose", "", "42"} {
			_, err := parseLogLevel(input)
			if err == nil {
				t.Errorf("parseLogLevel(%q) expected error, got nil", input)
			}
		}
	})
}

// --- AC-1.6: ToolCoverage ---

func TestToolCoverage(t *testing.T) {
	t.Run("all tools in param extractor map", func(t *testing.T) {
		for _, name := range allToolNames {
			if _, ok := toolParamExtractors[name]; !ok {
				t.Errorf("tool %q missing from toolParamExtractors", name)
			}
		}
	})

	t.Run("all tools in result extractor map", func(t *testing.T) {
		for _, name := range allToolNames {
			if _, ok := toolResultExtractors[name]; !ok {
				t.Errorf("tool %q missing from toolResultExtractors", name)
			}
		}
	})

	t.Run("no extra entries in param map", func(t *testing.T) {
		nameSet := make(map[string]bool)
		for _, n := range allToolNames {
			nameSet[n] = true
		}
		for name := range toolParamExtractors {
			if !nameSet[name] {
				t.Errorf("unexpected tool %q in toolParamExtractors", name)
			}
		}
	})

	t.Run("no extra entries in result map", func(t *testing.T) {
		nameSet := make(map[string]bool)
		for _, n := range allToolNames {
			nameSet[n] = true
		}
		for name := range toolResultExtractors {
			if !nameSet[name] {
				t.Errorf("unexpected tool %q in toolResultExtractors", name)
			}
		}
	})

	t.Run("param extractors produce attrs for typical args", func(t *testing.T) {
		typicalArgs := map[string]map[string]any{
			"create_entities":     {"entities": []any{map[string]any{"name": "Scout", "entityType": "Person"}}},
			"create_relations":    {"relations": []any{map[string]any{"from": "A", "to": "B", "relationType": "knows"}}},
			"add_observations":    {"observations": []any{map[string]any{"entityName": "Scout", "contents": []any{"obs1"}}}},
			"delete_entities":     {"entityNames": []any{"Scout"}},
			"delete_observations": {"deletions": []any{map[string]any{"entityName": "Scout", "observations": []any{"old"}}}},
			"delete_relations":    {"relations": []any{map[string]any{"from": "A", "to": "B", "relationType": "knows"}}},
			"read_graph":          {"mode": "summary"},
			"search_nodes":        {"query": "test"},
			"open_nodes":          {"names": []any{"Scout"}},
			"merge_entities":      {"sourceName": "Old", "targetName": "New"},
			"update_entities":     {"name": "Scout", "entityType": "Person"},
			"update_observations": {"entityName": "Scout"},
			"detect_conflicts":    {"entityName": "Scout"},
		}
		for _, name := range allToolNames {
			pe := toolParamExtractors[name]
			attrs := pe(typicalArgs[name])
			if len(attrs) == 0 {
				t.Errorf("tool %q param extractor returned no attrs for typical args", name)
			}
		}
	})

	t.Run("result extractors produce attrs for typical results", func(t *testing.T) {
		typicalResults := map[string]string{
			"create_entities":     `[{"name":"Scout","entityType":"Person","observations":[]}]`,
			"create_relations":    `[{"from":"A","to":"B","relationType":"knows"}]`,
			"add_observations":    `[{"entityName":"Scout","addedObservations":["obs1","obs2"]}]`,
			"delete_entities":     `Entities deleted successfully`,
			"delete_observations": `Observations deleted successfully`,
			"delete_relations":    `Relations deleted successfully`,
			"read_graph":          `{"totalEntities":5,"totalRelations":2,"entityTypes":{},"relationTypes":{},"entities":[],"limit":50,"hasMore":false}`,
			"search_nodes":        `{"entities":[],"total":3,"limit":0,"hasMore":false}`,
			"open_nodes":          `{"entities":[{"name":"Scout","observations":["a","b"]}],"relations":[]}`,
			"merge_entities":      `{"mergedObservations":3,"mergedRelations":1,"sourceDeleted":true}`,
			"update_entities":     `Entity "Scout" type updated to "Agent"`,
			"update_observations": `Observation updated successfully`,
			"detect_conflicts":    noConflictsMessage,
		}
		for _, name := range allToolNames {
			re := toolResultExtractors[name]
			result := mcp.NewToolResultText(typicalResults[name])
			attrs := re(result)
			if len(attrs) == 0 {
				t.Errorf("tool %q result extractor returned no attrs for typical result", name)
			}
		}
	})
}

// --- AC-1.7: FaultTolerant ---

func TestFaultTolerant(t *testing.T) {
	t.Run("nil result does not panic", func(t *testing.T) {
		for name, re := range toolResultExtractors {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("tool %q result extractor panicked on nil: %v", name, r)
					}
				}()
				re(nil)
			}()
		}
	})

	t.Run("empty content does not panic", func(t *testing.T) {
		emptyResult := &mcp.CallToolResult{}
		for name, re := range toolResultExtractors {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("tool %q result extractor panicked on empty content: %v", name, r)
					}
				}()
				re(emptyResult)
			}()
		}
	})

	t.Run("malformed JSON result does not panic", func(t *testing.T) {
		badResult := mcp.NewToolResultText("{not valid json!!")
		for name, re := range toolResultExtractors {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("tool %q result extractor panicked on bad JSON: %v", name, r)
					}
				}()
				re(badResult)
			}()
		}
	})

	t.Run("nil args do not panic param extractors", func(t *testing.T) {
		for name, pe := range toolParamExtractors {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("tool %q param extractor panicked on nil args: %v", name, r)
					}
				}()
				pe(nil)
			}()
		}
	})

	t.Run("wrong type args do not panic param extractors", func(t *testing.T) {
		wrongArgs := map[string]any{
			"entities":     "not an array",
			"relations":    42,
			"observations": true,
			"entityNames":  map[string]any{},
			"names":        nil,
			"query":        123,
			"mode":         []any{},
			"sourceName":   nil,
			"targetName":   nil,
			"entityName":   false,
			"name":         nil,
			"entityType":   nil,
			"deletions":    "wrong",
		}
		for name, pe := range toolParamExtractors {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("tool %q param extractor panicked on wrong types: %v", name, r)
					}
				}()
				pe(wrongArgs)
			}()
		}
	})

	t.Run("wrapper does not panic on nil args", func(t *testing.T) {
		logger, _ := testLogger(slog.LevelDebug)
		wrapped := withLogging(logger, "open_nodes", successHandler(`{"entities":[],"relations":[]}`))

		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("wrapper panicked on nil args: %v", r)
			}
		}()

		req := mcp.CallToolRequest{
			Params: mcp.CallToolParams{Arguments: nil},
		}
		wrapped(context.Background(), req)
	})
}
