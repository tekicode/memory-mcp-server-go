package storage

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// logRecord captures a single slog record for test assertions.
type logRecord struct {
	Level   slog.Level
	Message string
	Attrs   map[string]string
}

// testLogHandler captures slog records for test assertions.
type testLogHandler struct {
	mu      sync.Mutex
	records []logRecord
}

func (h *testLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return true // capture all levels
}

func (h *testLogHandler) Handle(_ context.Context, r slog.Record) error {
	rec := logRecord{
		Level:   r.Level,
		Message: r.Message,
		Attrs:   make(map[string]string),
	}
	r.Attrs(func(a slog.Attr) bool {
		rec.Attrs[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, rec)
	h.mu.Unlock()
	return nil
}

func (h *testLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *testLogHandler) WithGroup(name string) slog.Handler       { return h }

func (h *testLogHandler) getRecords() []logRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]logRecord, len(h.records))
	copy(cp, h.records)
	return cp
}

// TestLogFTSInitFailure verifies that a WARN-level log is emitted when FTS
// initialization fails (e.g., because the FTS5 extension is unavailable or
// the schema is corrupted).
func TestLogFTSInitFailure(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "log_fts_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Install test log handler
	handler := &testLogHandler{}
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(handler))
	defer slog.SetDefault(oldLogger)

	// Create a valid SQLite storage
	config := Config{
		FilePath:    filepath.Join(tempDir, "test.db"),
		WALMode:     true,
		CacheSize:   1000,
		BusyTimeout: 5 * time.Second,
	}
	store, err := NewSQLiteStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Initialize(); err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	store.Close()

	// Create a second DB where FTS creation will fail.
	// Drop FTS tables and create a conflicting regular table so
	// CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts fails.
	db2, err := sql.Open("sqlite", config.FilePath)
	if err != nil {
		t.Fatalf("Failed to open db for corruption: %v", err)
	}
	// Drop FTS tables and triggers, then create a regular table with the same name
	for _, q := range []string{
		"DROP TRIGGER IF EXISTS entities_fts_insert",
		"DROP TRIGGER IF EXISTS entities_fts_delete",
		"DROP TRIGGER IF EXISTS entities_fts_update",
		"DROP TRIGGER IF EXISTS observations_fts_insert",
		"DROP TRIGGER IF EXISTS observations_fts_delete",
		"DROP TRIGGER IF EXISTS observations_fts_update",
		"DROP TABLE IF EXISTS entities_fts",
		"DROP TABLE IF EXISTS observations_fts",
		// Create a regular table that conflicts with the FTS5 virtual table
		"CREATE TABLE entities_fts (name TEXT)",
	} {
		db2.Exec(q)
	}
	db2.Close()

	// Re-initialize — createFTSSchema() should fail because entities_fts
	// already exists as a regular table, not a virtual FTS5 table
	store2, err := NewSQLiteStorage(config)
	if err != nil {
		t.Fatalf("Failed to create second storage: %v", err)
	}
	_ = store2.Initialize()
	defer store2.Close()

	// Check for any WARN-level log about FTS
	records := handler.getRecords()
	hasWarn := false
	for _, r := range records {
		if r.Level == slog.LevelWarn && (strings.Contains(strings.ToLower(r.Message), "fts") ||
			containsValue(r.Attrs, "fts")) {
			hasWarn = true
			t.Logf("Found expected WARN log: %s", r.Message)
			break
		}
	}

	if !hasWarn {
		// Log all captured records for debugging
		for _, r := range records {
			t.Logf("  [%s] %s %v", r.Level, r.Message, r.Attrs)
		}
		t.Error("Expected WARN-level log about FTS failure, but none found")
	}
}

// TestLogPRAGMASuccess verifies that no WARN-level PRAGMA logs are emitted
// when read connection PRAGMA configuration succeeds (no false positives).
func TestLogPRAGMASuccess(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "log_pragma_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Install test log handler
	handler := &testLogHandler{}
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(handler))
	defer slog.SetDefault(oldLogger)

	// Normal initialization — PRAGMAs should succeed and NOT log warnings
	config := Config{
		FilePath:    filepath.Join(tempDir, "test.db"),
		WALMode:     true,
		CacheSize:   1000,
		BusyTimeout: 5 * time.Second,
	}
	store, err := NewSQLiteStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Initialize(); err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	// On successful init, PRAGMA warnings should NOT appear
	records := handler.getRecords()
	pragmaWarns := 0
	for _, r := range records {
		if r.Level == slog.LevelWarn && strings.Contains(strings.ToLower(r.Message), "pragma") {
			pragmaWarns++
		}
	}
	if pragmaWarns > 0 {
		t.Errorf("Expected no PRAGMA warnings on successful init, got %d", pragmaWarns)
	}
}

// TestLogSearchDebugSuccess verifies that no DEBUG-level error logs are emitted
// during a successful search (no false positives from the logging additions).
func TestLogSearchDebugSuccess(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "log_search_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Install test log handler
	handler := &testLogHandler{}
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(handler))
	defer slog.SetDefault(oldLogger)

	config := Config{
		FilePath:    filepath.Join(tempDir, "test.db"),
		WALMode:     true,
		CacheSize:   1000,
		BusyTimeout: 5 * time.Second,
	}
	store, err := NewSQLiteStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Initialize(); err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	// Create entities so search has something to find
	_, err = store.CreateEntities([]Entity{
		{Name: "TestEntity", EntityType: "test", Observations: []string{"test observation"}},
	})
	if err != nil {
		t.Fatalf("Failed to create entity: %v", err)
	}

	// Normal search should succeed without DEBUG errors
	_, err = store.SearchNodes("TestEntity", 10)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	// Verify no DEBUG-level error logs emitted for successful search.
	// Error logs are identified by the presence of an "error" attribute
	// or "failed" in the message, matching how production code logs errors.
	records := handler.getRecords()
	searchDebugErrors := 0
	for _, r := range records {
		if r.Level == slog.LevelDebug {
			if _, hasErr := r.Attrs["error"]; hasErr || strings.Contains(r.Message, "failed") {
				searchDebugErrors++
				t.Logf("  unexpected DEBUG error log: %s %v", r.Message, r.Attrs)
			}
		}
	}
	if searchDebugErrors > 0 {
		t.Fatalf("Expected no DEBUG-level error logs during successful search, got %d", searchDebugErrors)
	}
}

func containsValue(m map[string]string, substr string) bool {
	for _, v := range m {
		if strings.Contains(strings.ToLower(v), substr) {
			return true
		}
	}
	return false
}
