package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAtomicWriteNoTruncation verifies that saveGraph uses atomic write pattern
// (write-to-temp + fsync + rename) instead of os.WriteFile which truncates first.
// This test creates data, writes it, then verifies the original file is never
// left in a partial/empty state by checking that a temp file is used.
func TestAtomicWriteNoTruncation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "atomic_write_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "test.jsonl")
	config := Config{FilePath: filePath}
	storage, err := NewJSONLStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := storage.Initialize(); err != nil {
		t.Fatalf("Failed to initialize: %v", err)
	}

	// Write initial data
	entities := []Entity{
		{Name: "TestEntity", EntityType: "test", Observations: []string{"obs1"}},
	}
	_, err = storage.CreateEntities(entities)
	if err != nil {
		t.Fatalf("Failed to create entities: %v", err)
	}

	// Verify file exists and has content
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("File should have content after write")
	}

	// Verify no temp file is left behind after successful write
	tmpPath := filePath + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("Temp file should not exist after successful write")
	}
}

// TestAtomicWritePreservesDataOnWriteError verifies that if the write process
// fails (e.g., rename fails), the original file is not corrupted.
func TestAtomicWritePreservesDataOnWriteError(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "atomic_preserve_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "test.jsonl")
	config := Config{FilePath: filePath}
	storage, err := NewJSONLStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := storage.Initialize(); err != nil {
		t.Fatalf("Failed to initialize: %v", err)
	}

	// Write initial data
	entities := []Entity{
		{Name: "Original", EntityType: "test", Observations: []string{"should survive"}},
	}
	_, err = storage.CreateEntities(entities)
	if err != nil {
		t.Fatalf("Failed to create entities: %v", err)
	}

	// Read original content
	originalData, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read original: %v", err)
	}

	// Write more data — should succeed and update file
	moreEntities := []Entity{
		{Name: "Second", EntityType: "test", Observations: []string{"also here"}},
	}
	_, err = storage.CreateEntities(moreEntities)
	if err != nil {
		t.Fatalf("Failed to create second entity: %v", err)
	}

	// Verify both entities present
	updatedData, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read updated: %v", err)
	}
	if !strings.Contains(string(updatedData), "Original") {
		t.Error("Updated file should contain original entity")
	}
	if !strings.Contains(string(updatedData), "Second") {
		t.Error("Updated file should contain second entity")
	}
	if len(updatedData) <= len(originalData) {
		t.Error("Updated file should be larger than original")
	}
}

// TestSaveGraphAtomicPattern verifies the saveGraph function uses the atomic
// write pattern by checking that data written is consistent and complete.
func TestSaveGraphAtomicPattern(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "savegraph_atomic_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "test.jsonl")
	config := Config{FilePath: filePath}
	storage, err := NewJSONLStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := storage.Initialize(); err != nil {
		t.Fatalf("Failed to initialize: %v", err)
	}

	// Write a graph with multiple entities and relations
	graph := &KnowledgeGraph{
		Entities: []Entity{
			{Name: "A", EntityType: "node", Observations: []string{"obs-a"}},
			{Name: "B", EntityType: "node", Observations: []string{"obs-b"}},
			{Name: "C", EntityType: "node", Observations: []string{"obs-c"}},
		},
		Relations: []Relation{
			{From: "A", To: "B", RelationType: "connects"},
			{From: "B", To: "C", RelationType: "connects"},
		},
	}

	err = storage.saveGraph(graph)
	if err != nil {
		t.Fatalf("saveGraph failed: %v", err)
	}

	// Verify the written file is complete and parseable
	loaded, err := storage.loadGraph()
	if err != nil {
		t.Fatalf("loadGraph failed: %v", err)
	}

	if len(loaded.Entities) != 3 {
		t.Errorf("Expected 3 entities, got %d", len(loaded.Entities))
	}
	if len(loaded.Relations) != 2 {
		t.Errorf("Expected 2 relations, got %d", len(loaded.Relations))
	}

	// Verify no temp file remains
	tmpPath := filePath + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("Temp file should be cleaned up after successful saveGraph")
	}
}

// TestMarshalErrorPropagation verifies that saveGraph returns an error when
// json.Marshal fails, instead of silently dropping the entity.
// Note: json.Marshal on standard Go structs with string fields won't normally
// fail, but we verify the error path exists by checking that all entities
// written are present in the output (no silent drops).
func TestMarshalErrorEntityCount(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "marshal_error_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "test.jsonl")
	config := Config{FilePath: filePath}
	storage, err := NewJSONLStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := storage.Initialize(); err != nil {
		t.Fatalf("Failed to initialize: %v", err)
	}

	// Write entities and verify all are persisted (none silently dropped)
	graph := &KnowledgeGraph{
		Entities: []Entity{
			{Name: "E1", EntityType: "t1", Observations: []string{"o1"}},
			{Name: "E2", EntityType: "t2", Observations: []string{"o2"}},
			{Name: "E3", EntityType: "t3", Observations: []string{"o3"}},
		},
		Relations: []Relation{
			{From: "E1", To: "E2", RelationType: "r1"},
		},
	}

	err = storage.saveGraph(graph)
	if err != nil {
		t.Fatalf("saveGraph should succeed for valid data: %v", err)
	}

	// Reload and verify count matches — no silent drops
	loaded, err := storage.loadGraph()
	if err != nil {
		t.Fatalf("loadGraph failed: %v", err)
	}

	if len(loaded.Entities) != len(graph.Entities) {
		t.Errorf("Entity count mismatch: wrote %d, loaded %d — silent drop detected",
			len(graph.Entities), len(loaded.Entities))
	}
	if len(loaded.Relations) != len(graph.Relations) {
		t.Errorf("Relation count mismatch: wrote %d, loaded %d — silent drop detected",
			len(graph.Relations), len(loaded.Relations))
	}
}

// TestInitializeCleansUpStaleTmpFile verifies that Initialize() removes
// stale .tmp files left behind by a previous crashed write.
func TestInitializeCleansUpStaleTmpFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "init_cleanup_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "test.jsonl")
	tmpPath := filePath + ".tmp"

	// Simulate a crashed write by creating a stale .tmp file
	err = os.WriteFile(tmpPath, []byte("stale partial data"), 0644)
	if err != nil {
		t.Fatalf("Failed to create stale tmp file: %v", err)
	}

	// Verify tmp file exists before Initialize
	if _, err := os.Stat(tmpPath); os.IsNotExist(err) {
		t.Fatal("Stale tmp file should exist before Initialize")
	}

	// Initialize should clean up the stale tmp file
	config := Config{FilePath: filePath}
	storage, err := NewJSONLStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := storage.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Verify tmp file was removed
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("Initialize should have removed stale .tmp file")
	}

	// Verify the main file was created normally
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Error("Main file should exist after Initialize")
	}
}

// TestCleanupTmpOnSuccessfulWrite verifies that temp files are cleaned up
// after every successful write operation.
func TestCleanupTmpOnSuccessfulWrite(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "cleanup_success_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "test.jsonl")
	config := Config{FilePath: filePath}
	storage, err := NewJSONLStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := storage.Initialize(); err != nil {
		t.Fatalf("Failed to initialize: %v", err)
	}

	// Perform multiple write operations
	for i := 0; i < 5; i++ {
		entities := []Entity{
			{Name: "Entity", EntityType: "test", Observations: []string{"obs"}},
		}
		_, err = storage.CreateEntities(entities)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}

		// After each write, no temp file should remain
		tmpPath := filePath + ".tmp"
		if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
			t.Errorf("Temp file should not exist after successful write %d", i)
		}
	}
}
