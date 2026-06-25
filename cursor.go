package leat

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// cursor is reader-local read-state: per-lane consumed-line counts, persisted
// outside the repo so it generates zero history and stays private. Lanes are
// append-only, so line N is immutable once written and a consumed count is a
// sufficient, compact cursor. (Lane-rewriting compaction — opt-in, off by
// default — would invalidate counts; such a consumer resets its cursor.)
type cursor struct {
	path     string
	consumed map[string]int
}

type cursorFile struct {
	Consumed map[string]int `json:"consumed"`
}

func loadCursor(path string) *cursor {
	c := &cursor{path: path, consumed: map[string]int{}}
	data, err := os.ReadFile(path)
	if err != nil {
		return c
	}
	var cf cursorFile
	if json.Unmarshal(data, &cf) == nil && cf.Consumed != nil {
		c.consumed = cf.Consumed
	}
	return c
}

func (c *cursor) save() error {
	if err := os.MkdirAll(filepath.Dir(c.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cursorFile{Consumed: c.consumed}, "", "  ")
	if err != nil {
		return err
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}
