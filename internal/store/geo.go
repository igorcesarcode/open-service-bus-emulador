package store

// GeoAlias is a Geo-DR alias row.
type GeoAlias struct {
	Alias, Primary, Secondary string
	Paired                    bool
}

// GeoAliases returns the Geo-DR aliases.
func (t *Tx) GeoAliases() ([]GeoAlias, error) {
	rows, err := t.query(`SELECT alias, primary_ns, secondary_ns, paired FROM geodr_aliases ORDER BY alias`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GeoAlias
	for rows.Next() {
		var a GeoAlias
		var paired int
		if err := rows.Scan(&a.Alias, &a.Primary, &a.Secondary, &paired); err != nil {
			return nil, err
		}
		a.Paired = paired != 0
		out = append(out, a)
	}
	return out, rows.Err()
}

// SaveGeoAlias inserts or replaces an alias.
func (t *Tx) SaveGeoAlias(a GeoAlias) error {
	paired := 0
	if a.Paired {
		paired = 1
	}
	_, err := t.exec(`INSERT OR REPLACE INTO geodr_aliases (alias, primary_ns, secondary_ns, paired) VALUES (?, ?, ?, ?)`,
		a.Alias, a.Primary, a.Secondary, paired)
	return err
}

// DeleteGeoAlias removes an alias.
func (t *Tx) DeleteGeoAlias(alias string) error {
	_, err := t.exec(`DELETE FROM geodr_aliases WHERE alias = ?`, alias)
	return err
}
