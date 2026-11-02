package store

import (
	"database/sql"
	"errors"
)

// Setting returns the value saved under key, or "" when there is none.
func (t *Tx) Setting(key string) (string, error) {
	var v string
	err := t.queryRow(`SELECT value FROM settings WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

// SetSetting saves value under key; an empty value removes the key.
func (t *Tx) SetSetting(key, value string) error {
	if value == "" {
		_, err := t.exec(`DELETE FROM settings WHERE key = ?`, key)
		return err
	}
	_, err := t.exec(`INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)`, key, value)
	return err
}
