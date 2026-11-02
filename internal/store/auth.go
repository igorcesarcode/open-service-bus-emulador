package store

// Policy is a shared access policy row.
type Policy struct {
	Scope, Name              string
	Rights                   int
	PrimaryKey, SecondaryKey string
}

// Policies returns every shared access policy.
func (t *Tx) Policies() ([]Policy, error) {
	rows, err := t.query(`SELECT scope, name, rights, primary_key, secondary_key FROM auth_policies ORDER BY scope, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Policy
	for rows.Next() {
		var p Policy
		if err := rows.Scan(&p.Scope, &p.Name, &p.Rights, &p.PrimaryKey, &p.SecondaryKey); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// SavePolicy inserts or replaces a policy.
func (t *Tx) SavePolicy(p Policy) error {
	_, err := t.exec(`INSERT OR REPLACE INTO auth_policies (scope, name, rights, primary_key, secondary_key) VALUES (?, ?, ?, ?, ?)`,
		p.Scope, p.Name, p.Rights, p.PrimaryKey, p.SecondaryKey)
	return err
}

// DeletePolicy removes a policy.
func (t *Tx) DeletePolicy(scope, name string) error {
	_, err := t.exec(`DELETE FROM auth_policies WHERE scope = ? AND name = ?`, scope, name)
	return err
}

// Assignment is a role assignment row.
type Assignment struct {
	ID                     int64
	Principal, Role, Scope string
}

// Assignments returns every role assignment.
func (t *Tx) Assignments() ([]Assignment, error) {
	rows, err := t.query(`SELECT id, principal, role, scope FROM role_assignments ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Assignment
	for rows.Next() {
		var a Assignment
		if err := rows.Scan(&a.ID, &a.Principal, &a.Role, &a.Scope); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// AddAssignment inserts an assignment unless it exists.
func (t *Tx) AddAssignment(a Assignment) error {
	_, err := t.exec(`INSERT OR IGNORE INTO role_assignments (principal, role, scope) VALUES (?, ?, ?)`, a.Principal, a.Role, a.Scope)
	return err
}

// DeleteAssignment removes an assignment by id.
func (t *Tx) DeleteAssignment(id int64) error {
	_, err := t.exec(`DELETE FROM role_assignments WHERE id = ?`, id)
	return err
}
