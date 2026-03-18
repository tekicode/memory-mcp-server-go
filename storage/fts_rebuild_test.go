package storage

import (
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// newTestSQLiteStorage creates a fresh SQLite storage for testing.
// Enables foreign keys so CASCADE deletes work correctly.
func newTestSQLiteStorage(t *testing.T) *SQLiteStorage {
	t.Helper()
	tempDir := t.TempDir()
	config := Config{
		FilePath:    filepath.Join(tempDir, "test.db"),
		WALMode:     true,
		CacheSize:   1000,
		BusyTimeout: 5 * time.Second,
	}
	s, err := NewSQLiteStorage(config)
	if err != nil {
		t.Fatalf("Failed to create SQLite storage: %v", err)
	}
	if err := s.Initialize(); err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	// Enable foreign keys for CASCADE delete support
	if _, err := s.db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// seedTestEntities populates the database with entities whose observation
// text contains a known search term ("portable memory") so we can verify
// that FTS indexes observation content (not entity_id integers).
func seedTestEntities(t *testing.T, s *SQLiteStorage) {
	t.Helper()
	entities := []Entity{
		{
			Name:         "Scout",
			EntityType:   "Person",
			Observations: []string{"Puppy AI assistant with portable memory"},
		},
		{
			Name:         "Teki",
			EntityType:   "Person",
			Observations: []string{"Transgender woman and bunny furry"},
		},
		{
			Name:         "Burrow",
			EntityType:   "Infrastructure",
			Observations: []string{"k3s cluster running on CachyOS"},
		},
	}
	if _, err := s.CreateEntities(entities); err != nil {
		t.Fatalf("Failed to seed entities: %v", err)
	}
}

// TestFTSRebuildCorrectness verifies that after a full FTS rebuild,
// searching for a term that appears in observation content returns the
// correct entity — not garbage from positional column mismatch.
//
// Before fix: observations_fts uses content='observations' which causes
// the rebuild command to read entity_id (integer) into the FTS "content"
// column and observation text into the FTS "entity_name" column.
// After fix: observations_fts is standalone; rebuild deletes + re-inserts
// with the correct JOIN.
func TestFTSRebuildCorrectness(t *testing.T) {
	s := newTestSQLiteStorage(t)
	seedTestEntities(t, s)

	// Verify search works BEFORE rebuild (triggers inserted correct data)
	result, err := s.SearchNodesWithFTS("portable", 10)
	if err != nil {
		t.Fatalf("Pre-rebuild FTS search failed: %v", err)
	}
	if result.Total == 0 {
		t.Fatal("Pre-rebuild: expected to find 'Scout' via observation 'portable memory', got 0 results")
	}

	// Clear the FTS table to force a full rebuild
	_, err = s.db.Exec("DELETE FROM observations_fts")
	if err != nil {
		t.Fatalf("Failed to clear observations_fts: %v", err)
	}

	// Rebuild FTS index
	if err := s.rebuildFTSIndex(); err != nil {
		t.Fatalf("rebuildFTSIndex failed: %v", err)
	}

	// Search for "portable" — should find Scout via observation content
	result, err = s.SearchNodesWithFTS("portable", 10)
	if err != nil {
		t.Fatalf("Post-rebuild FTS search failed: %v", err)
	}
	if result.Total == 0 {
		t.Fatal("Post-rebuild: expected to find 'Scout' via observation 'portable memory', got 0 results")
	}

	found := false
	for _, hit := range result.Entities {
		if hit.Name == "Scout" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Post-rebuild: expected 'Scout' in results, got: %v", result.Entities)
	}

	// Verify entity_name column is correctly populated by searching for
	// entity name — this catches the case where entity_name got observation
	// content instead (the column swap).
	result, err = s.SearchNodesWithFTS("Teki", 10)
	if err != nil {
		t.Fatalf("Post-rebuild entity name search failed: %v", err)
	}
	foundTeki := false
	for _, hit := range result.Entities {
		if hit.Name == "Teki" {
			foundTeki = true
			break
		}
	}
	if !foundTeki {
		t.Errorf("Post-rebuild: expected 'Teki' found via entity name in observations_fts, got: %v", result.Entities)
	}
}

// TestFTSRebuildDoesNotCorruptWithIntegerIDs specifically verifies that
// FTS rebuild does not inject integer entity_id values into the text search
// index. This is the core symptom of the content-sync column mismatch.
func TestFTSRebuildDoesNotCorruptWithIntegerIDs(t *testing.T) {
	s := newTestSQLiteStorage(t)
	seedTestEntities(t, s)

	// Clear and rebuild
	if _, err := s.db.Exec("DELETE FROM observations_fts"); err != nil {
		t.Fatalf("Failed to clear observations_fts: %v", err)
	}
	if err := s.rebuildFTSIndex(); err != nil {
		t.Fatalf("rebuildFTSIndex failed: %v", err)
	}

	// Verify FTS content is correct by querying the FTS table directly.
	// If the column mismatch bug exists, entity_id integers would be
	// stored in the "content" column instead of observation text.
	var ftsContent string
	err := s.db.QueryRow("SELECT content FROM observations_fts LIMIT 1").Scan(&ftsContent)
	if err != nil {
		t.Fatalf("Failed to read FTS content: %v", err)
	}

	// The content should be observation text, not an integer.
	// If column mismatch exists, entity_id integers leak into the content column.
	if _, err := strconv.Atoi(ftsContent); err == nil {
		t.Errorf("FTS content column contains numeric value '%s' — column mismatch bug is present (entity_id leaked into content)", ftsContent)
	}
	t.Logf("FTS content column contains: %q (correct — non-numeric text)", ftsContent)
}

// TestFTSDeleteObservationDirect verifies that deleting an observation
// directly (not via entity CASCADE) removes it from the FTS index.
func TestFTSDeleteObservationDirect(t *testing.T) {
	s := newTestSQLiteStorage(t)
	seedTestEntities(t, s)

	// Verify observation is searchable
	result, err := s.SearchNodes("portable", 10)
	if err != nil {
		t.Fatalf("Pre-delete search failed: %v", err)
	}
	if result.Total == 0 {
		t.Fatal("Pre-delete: expected to find 'Scout' via 'portable memory'")
	}

	// Delete the observation directly
	err = s.DeleteObservations([]ObservationDeletion{
		{
			EntityName:   "Scout",
			Observations: []string{"Puppy AI assistant with portable memory"},
		},
	})
	if err != nil {
		t.Fatalf("DeleteObservations failed: %v", err)
	}

	// Search should no longer find "portable"
	result, err = s.SearchNodes("portable", 10)
	if err != nil {
		t.Fatalf("Post-delete search failed: %v", err)
	}
	for _, hit := range result.Entities {
		if hit.Name == "Scout" {
			t.Errorf("Post-delete: 'Scout' should not match 'portable' after observation deleted, but found in results")
		}
	}

	// Other entities should still be searchable
	result, err = s.SearchNodes("bunny", 10)
	if err != nil {
		t.Fatalf("Post-delete search for other entity failed: %v", err)
	}
	if result.Total == 0 {
		t.Error("Post-delete: expected 'Teki' still searchable via 'bunny'")
	}
}

// TestFTSDeleteEntityCascade verifies that deleting an entity (which
// CASCADE-deletes its observations) correctly removes entries from the
// FTS index. This is the critical path: the CASCADE fires the
// observations_fts_delete trigger while the entity row is being deleted.
//
// Before fix: the delete trigger JOINs to entities to get entity_name,
// but the entity is already being deleted — the JOIN may return nothing,
// leaving orphaned FTS entries.
// After fix: the delete trigger uses DELETE FROM observations_fts WHERE
// rowid = old.id — no JOIN needed, works correctly during CASCADE.
func TestFTSDeleteEntityCascade(t *testing.T) {
	s := newTestSQLiteStorage(t)
	seedTestEntities(t, s)

	// Verify Scout is searchable via observation content
	result, err := s.SearchNodes("portable", 10)
	if err != nil {
		t.Fatalf("Pre-delete search failed: %v", err)
	}
	if result.Total == 0 {
		t.Fatal("Pre-delete: expected to find 'Scout' via 'portable memory'")
	}

	// Delete the entity — CASCADE should remove observations too
	err = s.DeleteEntities([]string{"Scout"})
	if err != nil {
		t.Fatalf("DeleteEntities failed: %v", err)
	}

	// FTS should no longer have Scout's observations
	result, err = s.SearchNodes("portable", 10)
	if err != nil {
		t.Fatalf("Post-cascade search failed: %v", err)
	}
	for _, hit := range result.Entities {
		if hit.Name == "Scout" {
			t.Errorf("Post-cascade: 'Scout' should not appear after entity deletion, but found in results")
		}
	}
	if result.Total != 0 {
		t.Errorf("Post-cascade: expected 0 results for 'portable', got %d", result.Total)
	}

	// Other entities should still be searchable
	result, err = s.SearchNodes("cluster", 10)
	if err != nil {
		t.Fatalf("Post-cascade search for other entity failed: %v", err)
	}
	foundBurrow := false
	for _, hit := range result.Entities {
		if hit.Name == "Burrow" {
			foundBurrow = true
		}
	}
	if !foundBurrow {
		t.Error("Post-cascade: expected 'Burrow' still searchable via 'cluster'")
	}
}

// TestFTSDeleteEntityCascadeNoOrphanedRows verifies that CASCADE delete
// does not leave orphaned rows in observations_fts by checking the row
// count directly.
func TestFTSDeleteEntityCascadeNoOrphanedRows(t *testing.T) {
	s := newTestSQLiteStorage(t)
	seedTestEntities(t, s)

	// Count FTS rows before delete
	var beforeCount int
	err := s.db.QueryRow("SELECT COUNT(*) FROM observations_fts").Scan(&beforeCount)
	if err != nil {
		t.Fatalf("Failed to count FTS rows: %v", err)
	}
	if beforeCount != 3 {
		t.Fatalf("Expected 3 FTS rows before delete, got %d", beforeCount)
	}

	// Delete one entity (has 1 observation) — CASCADE deletes observations,
	// which fires the observations_fts_delete trigger
	err = s.DeleteEntities([]string{"Scout"})
	if err != nil {
		t.Fatalf("DeleteEntities failed: %v", err)
	}

	// Count FTS rows after delete — should be 2
	var afterCount int
	err = s.db.QueryRow("SELECT COUNT(*) FROM observations_fts").Scan(&afterCount)
	if err != nil {
		t.Fatalf("Failed to count FTS rows after delete: %v", err)
	}
	if afterCount != 2 {
		t.Errorf("Expected 2 FTS rows after deleting Scout (had 1 obs), got %d", afterCount)
	}
}

// TestFTSExistingSearchPriorityUnchanged is a meta-test verifying that
// the FTS changes don't break the existing search priority behavior.
func TestFTSExistingSearchPriorityUnchanged(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fts_priority_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := Config{
		FilePath:    filepath.Join(tempDir, "test.db"),
		WALMode:     true,
		CacheSize:   1000,
		BusyTimeout: 5 * time.Second,
	}
	storage, err := NewSQLiteStorage(config)
	if err != nil {
		t.Fatalf("Failed to create SQLite storage: %v", err)
	}
	if err := storage.Initialize(); err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()

	// Create entities where "Claude" appears in name vs observation
	entities := []Entity{
		{
			Name:         "Claude Code",
			EntityType:   "tool",
			Observations: []string{"A CLI tool for coding assistance"},
		},
		{
			Name:         "VSCode",
			EntityType:   "editor",
			Observations: []string{"Supports Claude plugin for AI assistance"},
		},
	}
	if _, err := storage.CreateEntities(entities); err != nil {
		t.Fatalf("Failed to create entities: %v", err)
	}

	result, err := storage.SearchNodes("Claude", 10)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if result.Total != 2 {
		t.Errorf("Expected 2 results, got %d", result.Total)
	}
	if len(result.Entities) >= 2 {
		if result.Entities[0].Name != "Claude Code" {
			t.Errorf("Expected name match 'Claude Code' first, got '%s'", result.Entities[0].Name)
		}
		if result.Entities[1].Name != "VSCode" {
			t.Errorf("Expected content match 'VSCode' second, got '%s'", result.Entities[1].Name)
		}
	}
}

// TestFTSMigrationFromOldSchema simulates the real-world upgrade path:
// a database created with the old content-sync observations_fts definition
// is opened with the new code, triggering migration. After migration,
// FTS search should return correct results (not corrupted integer IDs).
func TestFTSMigrationFromOldSchema(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "migrate.db")

	// Phase 1: Create a database with the OLD content-sync schema manually.
	// We simulate what the old code would have created.
	db, err := openTestDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	// Create base tables (same as createSchema)
	_, err = db.Exec(`
		PRAGMA foreign_keys = ON;

		CREATE TABLE IF NOT EXISTS entities (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			entity_type TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS observations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			entity_id INTEGER NOT NULL,
			content TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
			UNIQUE(entity_id, content)
		);

		CREATE TABLE IF NOT EXISTS relations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			from_entity_id INTEGER NOT NULL,
			to_entity_id INTEGER NOT NULL,
			relation_type TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (from_entity_id) REFERENCES entities(id) ON DELETE CASCADE,
			FOREIGN KEY (to_entity_id) REFERENCES entities(id) ON DELETE CASCADE,
			UNIQUE(from_entity_id, to_entity_id, relation_type)
		);

		CREATE TABLE IF NOT EXISTS metadata (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);
		INSERT OR IGNORE INTO metadata (key, value) VALUES ('schema_version', '1.0');
	`)
	if err != nil {
		t.Fatalf("Failed to create base schema: %v", err)
	}

	// Create the OLD content-sync observations_fts (the buggy version)
	_, err = db.Exec(`
		CREATE VIRTUAL TABLE IF NOT EXISTS observations_fts USING fts5(
			content,
			entity_name,
			content='observations',
			content_rowid='id',
			tokenize='porter unicode61 remove_diacritics 1'
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create old FTS schema: %v", err)
	}

	// Insert test data directly (bypassing triggers since we're simulating old data)
	_, err = db.Exec(`
		INSERT INTO entities (name, entity_type) VALUES ('Alice', 'Person');
		INSERT INTO entities (name, entity_type) VALUES ('Bob', 'Person');
		INSERT INTO observations (entity_id, content)
			SELECT id, 'Works on distributed systems' FROM entities WHERE name = 'Alice';
		INSERT INTO observations (entity_id, content)
			SELECT id, 'Expert in database internals' FROM entities WHERE name = 'Bob';
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Manually populate FTS with CORRECT data (simulating triggers from old code)
	_, err = db.Exec(`
		INSERT INTO observations_fts(rowid, content, entity_name)
		SELECT o.id, o.content, e.name
		FROM observations o JOIN entities e ON o.entity_id = e.id;
	`)
	if err != nil {
		t.Fatalf("Failed to populate old FTS: %v", err)
	}

	db.Close()

	// Phase 2: Open the database with the NEW code (triggers migration).
	config := Config{
		FilePath:    dbPath,
		WALMode:     true,
		CacheSize:   1000,
		BusyTimeout: 5 * time.Second,
	}
	s, err := NewSQLiteStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := s.Initialize(); err != nil {
		t.Fatalf("Failed to initialize (migration): %v", err)
	}
	defer s.Close()

	// Phase 3: Verify FTS search works correctly after migration.
	// Search for "distributed" — should find Alice via observation content.
	result, err := s.SearchNodes("distributed", 10)
	if err != nil {
		t.Fatalf("Post-migration search failed: %v", err)
	}
	if result.Total == 0 {
		t.Fatal("Post-migration: expected to find 'Alice' via 'distributed systems', got 0 results")
	}
	found := false
	for _, hit := range result.Entities {
		if hit.Name == "Alice" {
			found = true
		}
	}
	if !found {
		t.Errorf("Post-migration: expected 'Alice' in results for 'distributed', got: %v", result.Entities)
	}

	// Search for "database" — should find Bob.
	result, err = s.SearchNodes("database", 10)
	if err != nil {
		t.Fatalf("Post-migration search for Bob failed: %v", err)
	}
	foundBob := false
	for _, hit := range result.Entities {
		if hit.Name == "Bob" {
			foundBob = true
		}
	}
	if !foundBob {
		t.Errorf("Post-migration: expected 'Bob' in results for 'database', got: %v", result.Entities)
	}

	// Verify the FTS table is now standalone (no content-sync)
	var sqlText string
	err = s.db.QueryRow("SELECT sql FROM sqlite_master WHERE name = 'observations_fts'").Scan(&sqlText)
	if err != nil {
		t.Fatalf("Failed to read FTS schema: %v", err)
	}
	if strings.Contains(sqlText, "content=") {
		t.Errorf("Post-migration: observations_fts still has content-sync: %s", sqlText)
	}
}

// openTestDB opens a raw SQLite connection for test setup.
func openTestDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}
