package store

import (
	"path/filepath"
	"testing"
)

// shouldPanic asserts that fn panics.
func shouldPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("expected panic but did not panic")
		}
	}()
	fn()
}

// tempFilePath returns a path inside t.TempDir() that does not exist yet.
func tempFilePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.db")
}
