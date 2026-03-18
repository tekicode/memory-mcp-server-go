package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// ToolHandler is the function signature for MCP tool handlers.
type ToolHandler = server.ToolHandlerFunc

// paramExtractor extracts key parameters from tool arguments for logging.
type paramExtractor func(args map[string]any) []slog.Attr

// resultExtractor extracts summary info from tool results for logging.
type resultExtractor func(result *mcp.CallToolResult) []slog.Attr

// withLogging wraps a tool handler with structured logging.
// The handler's return values are passed through exactly — logging is a side effect only.
func withLogging(logger *slog.Logger, name string, handler ToolHandler) ToolHandler {
	return func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		start := time.Now()

		// DEBUG: log full request args before handler execution
		if logger.Enabled(ctx, slog.LevelDebug) {
			argsJSON, _ := json.Marshal(request.GetRawArguments())
			logger.LogAttrs(ctx, slog.LevelDebug, name+".request",
				slog.String("args", string(argsJSON)),
			)
		}

		result, err := handler(ctx, request)
		duration := time.Since(start)

		if err != nil {
			var attrs []slog.Attr
			if pe, ok := toolParamExtractors[name]; ok {
				attrs = append(attrs, pe(request.GetArguments())...)
			}
			attrs = append(attrs, slog.Duration("duration", duration), slog.Any("error", err))
			logger.LogAttrs(ctx, slog.LevelError, name, attrs...)
			return result, err
		}

		// INFO: tool name, key params, duration, result summary
		if !logger.Enabled(ctx, slog.LevelInfo) {
			return result, nil
		}
		var attrs []slog.Attr
		if pe, ok := toolParamExtractors[name]; ok {
			attrs = append(attrs, pe(request.GetArguments())...)
		}
		attrs = append(attrs, slog.Duration("duration", duration))
		if re, ok := toolResultExtractors[name]; ok {
			attrs = append(attrs, re(result)...)
		}

		logger.LogAttrs(ctx, slog.LevelInfo, name, attrs...)
		return result, nil
	}
}

// parseLogLevel converts a string log level to slog.Level.
func parseLogLevel(level string) (slog.Level, error) {
	switch strings.ToLower(level) {
	case "error":
		return slog.LevelError, nil
	case "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	default:
		return 0, fmt.Errorf("invalid log level %q: must be error, info, or debug", level)
	}
}

// --- Helpers ---

// resultText extracts the text content from a CallToolResult.
func resultText(result *mcp.CallToolResult) string {
	if result == nil || len(result.Content) == 0 {
		return ""
	}
	if tc, ok := result.Content[0].(mcp.TextContent); ok {
		return tc.Text
	}
	return ""
}

// --- Shared param extractor helpers ---

// extractStringSlice extracts a string slice from args[field] and logs it with the given label.
func extractStringSlice(field, label string) paramExtractor {
	return func(args map[string]any) []slog.Attr {
		if args == nil {
			return nil
		}
		if v, ok := args[field]; ok {
			if arr, ok := v.([]any); ok {
				names := make([]string, 0, len(arr))
				for _, item := range arr {
					if s, ok := item.(string); ok {
						names = append(names, s)
					}
				}
				return []slog.Attr{slog.Any(label, names)}
			}
		}
		return nil
	}
}

// extractNamesFromObjects extracts a string field from each object in args[arrayField].
func extractNamesFromObjects(arrayField, nameField, label string) paramExtractor {
	return func(args map[string]any) []slog.Attr {
		if args == nil {
			return nil
		}
		if v, ok := args[arrayField]; ok {
			if arr, ok := v.([]any); ok {
				names := make([]string, 0, len(arr))
				for _, item := range arr {
					if obj, ok := item.(map[string]any); ok {
						if name, ok := obj[nameField].(string); ok {
							names = append(names, name)
						}
					}
				}
				return []slog.Attr{slog.Any(label, names)}
			}
		}
		return nil
	}
}

// extractString extracts a single string field from args.
func extractString(field string) paramExtractor {
	return func(args map[string]any) []slog.Attr {
		if args == nil {
			return nil
		}
		if v, ok := args[field]; ok {
			if s, ok := v.(string); ok {
				return []slog.Attr{slog.String(field, s)}
			}
		}
		return nil
	}
}

// extractStrings extracts multiple string fields from args.
func extractStrings(fields ...string) paramExtractor {
	return func(args map[string]any) []slog.Attr {
		if args == nil {
			return nil
		}
		var attrs []slog.Attr
		for _, field := range fields {
			if v, ok := args[field]; ok {
				if s, ok := v.(string); ok {
					attrs = append(attrs, slog.String(field, s))
				}
			}
		}
		return attrs
	}
}

// extractArrayLen logs the count of items in an array field.
func extractArrayLen(field, label string) paramExtractor {
	return func(args map[string]any) []slog.Attr {
		if args == nil {
			return nil
		}
		if v, ok := args[field]; ok {
			if arr, ok := v.([]any); ok {
				return []slog.Attr{slog.Int(label, len(arr))}
			}
		}
		return nil
	}
}

// --- Shared result extractor helpers ---

// extractJSONArrayLen counts items in a JSON array result.
func extractJSONArrayLen(label string) resultExtractor {
	return func(result *mcp.CallToolResult) []slog.Attr {
		text := resultText(result)
		var arr []json.RawMessage
		if json.Unmarshal([]byte(text), &arr) == nil {
			return []slog.Attr{slog.Int(label, len(arr))}
		}
		return nil
	}
}

// extractResultOK returns a simple ok status for operations that return text confirmation.
func extractResultOK() resultExtractor {
	return func(result *mcp.CallToolResult) []slog.Attr {
		return []slog.Attr{slog.String("result", "ok")}
	}
}

// --- Per-tool extractor maps ---

var toolParamExtractors = map[string]paramExtractor{
	"create_entities":     extractNamesFromObjects("entities", "name", "entities"),
	"create_relations":    extractArrayLen("relations", "count"),
	"add_observations":    extractNamesFromObjects("observations", "entityName", "entities"),
	"delete_entities":     extractStringSlice("entityNames", "names"),
	"delete_observations": extractNamesFromObjects("deletions", "entityName", "entities"),
	"delete_relations":    extractArrayLen("relations", "count"),
	"read_graph":          extractString("mode"),
	"search_nodes":        extractString("query"),
	"open_nodes":          extractStringSlice("names", "names"),
	"merge_entities":      extractStrings("sourceName", "targetName"),
	"update_entities":     extractStrings("name", "entityType"),
	"update_observations": extractString("entityName"),
	"detect_conflicts":    extractString("entityName"),
}

var toolResultExtractors = map[string]resultExtractor{
	"create_entities":     extractJSONArrayLen("created"),
	"create_relations":    extractJSONArrayLen("created"),
	"add_observations":    extractAddedObservations,
	"delete_entities":     extractResultOK(),
	"delete_observations": extractResultOK(),
	"delete_relations":    extractResultOK(),
	"read_graph":          extractReadGraphSummary,
	"search_nodes":        extractSearchResults,
	"open_nodes":          extractOpenNodesSummary,
	"merge_entities":      extractMergeResult,
	"update_entities":     extractResultOK(),
	"update_observations": extractResultOK(),
	"detect_conflicts":    extractConflictsSummary,
}

// noConflictsMessage is the text returned by detect_conflicts when no conflicts are found.
// Shared between the handler (main.go) and the result extractor to keep them in sync.
const noConflictsMessage = "No conflicts detected"

// --- Custom result extractors ---

func extractAddedObservations(result *mcp.CallToolResult) []slog.Attr {
	text := resultText(result)
	var results []struct {
		AddedObservations []string `json:"addedObservations"`
	}
	if json.Unmarshal([]byte(text), &results) == nil {
		total := 0
		for _, r := range results {
			total += len(r.AddedObservations)
		}
		return []slog.Attr{slog.Int("added", total)}
	}
	return nil
}

func extractReadGraphSummary(result *mcp.CallToolResult) []slog.Attr {
	text := resultText(result)
	// Try summary format (uses pointer to distinguish present-but-zero from absent)
	var summary struct {
		TotalEntities *int `json:"totalEntities"`
	}
	if json.Unmarshal([]byte(text), &summary) == nil && summary.TotalEntities != nil {
		return []slog.Attr{slog.Int("entities", *summary.TotalEntities)}
	}
	// Try full graph format
	var graph struct {
		Entities []json.RawMessage `json:"entities"`
	}
	if json.Unmarshal([]byte(text), &graph) == nil {
		return []slog.Attr{slog.Int("entities", len(graph.Entities))}
	}
	return nil
}

func extractSearchResults(result *mcp.CallToolResult) []slog.Attr {
	text := resultText(result)
	var sr struct {
		Total int `json:"total"`
	}
	if json.Unmarshal([]byte(text), &sr) == nil {
		return []slog.Attr{slog.Int("results", sr.Total)}
	}
	return nil
}

func extractOpenNodesSummary(result *mcp.CallToolResult) []slog.Attr {
	text := resultText(result)
	var graph struct {
		Entities []struct {
			Observations []json.RawMessage `json:"observations"`
		} `json:"entities"`
	}
	if json.Unmarshal([]byte(text), &graph) == nil {
		obsCount := 0
		for _, e := range graph.Entities {
			obsCount += len(e.Observations)
		}
		return []slog.Attr{
			slog.Int("entities", len(graph.Entities)),
			slog.Int("observations", obsCount),
		}
	}
	return nil
}

func extractMergeResult(result *mcp.CallToolResult) []slog.Attr {
	text := resultText(result)
	var mr struct {
		MergedObservations int `json:"mergedObservations"`
		MergedRelations    int `json:"mergedRelations"`
	}
	if json.Unmarshal([]byte(text), &mr) == nil {
		return []slog.Attr{
			slog.Int("mergedObservations", mr.MergedObservations),
			slog.Int("mergedRelations", mr.MergedRelations),
		}
	}
	return nil
}

func extractConflictsSummary(result *mcp.CallToolResult) []slog.Attr {
	text := resultText(result)
	if text == noConflictsMessage {
		return []slog.Attr{slog.Int("conflicts", 0)}
	}
	var conflicts []json.RawMessage
	if json.Unmarshal([]byte(text), &conflicts) == nil {
		return []slog.Attr{slog.Int("conflicts", len(conflicts))}
	}
	return nil
}
