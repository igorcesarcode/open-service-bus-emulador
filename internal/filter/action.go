package filter

import (
	"time"
)

// SQLAction is a parsed SQL rule action: SET and REMOVE statements that
// change the properties of a message copy.
type SQLAction struct {
	stmts  []statement
	text   string
	params Values
}

// WithParameters returns a copy of a that resolves @name references to
// params.
func (a *SQLAction) WithParameters(params Values) *SQLAction {
	c := *a
	c.params = params
	return &c
}

type statement struct {
	remove bool
	prop   *propRef
	value  node
}

// ParseSQLAction parses a rule action such as
// "SET quantity = quantity / 2; REMOVE color". Statements are separated
// by optional semicolons.
func ParseSQLAction(expr string) (*SQLAction, error) {
	toks, err := lex(expr)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	a := &SQLAction{text: expr}
	for {
		for p.acceptOp(";") {
		}
		if p.peek().kind == tokEOF {
			break
		}
		switch {
		case p.acceptKeyword("SET"):
			prop, err := p.parseProperty()
			if err != nil {
				return nil, err
			}
			if err := p.expectOp("="); err != nil {
				return nil, err
			}
			v, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			a.stmts = append(a.stmts, statement{prop: prop, value: v})
		case p.acceptKeyword("REMOVE"):
			prop, err := p.parseProperty()
			if err != nil {
				return nil, err
			}
			a.stmts = append(a.stmts, statement{remove: true, prop: prop})
		default:
			return nil, p.errorf("expected SET or REMOVE, found %v", p.peek())
		}
	}
	if len(a.stmts) == 0 {
		return nil, &SyntaxError{Msg: "empty action"}
	}
	return a, nil
}

// String returns the action text.
func (a *SQLAction) String() string { return a.text }

// Apply runs the statements in order on m. It returns an *EvalError when a
// statement cannot be applied, such as one that sets a read-only system
// property; Service Bus dead-letters the message then.
func (a *SQLAction) Apply(target MutableMessage) error {
	m := &paramMutableMessage{MutableMessage: target, params: a.params}
	for _, st := range a.stmts {
		if st.remove {
			if st.prop.scope == System {
				return evalErrorf("REMOVE sys.%s: system properties cannot be removed", st.prop.name)
			}
			m.RemoveProperty(st.prop.name)
			continue
		}
		v, err := st.value.eval(m)
		if err != nil {
			return err
		}
		if v == unknown {
			v = nil
		}
		if _, ok := v.(opaque); ok {
			return evalErrorf("SET %s: unsupported value type", st.prop.name)
		}
		if st.prop.scope == System {
			canonical, ok := SystemProperty(st.prop.name)
			if !ok {
				return evalErrorf("unknown system property sys.%s", st.prop.name)
			}
			if !settable[canonical] {
				return evalErrorf("system property sys.%s cannot be set", canonical)
			}
			if canonical == "ScheduledEnqueueTimeUtc" {
				// Ignored on subscriptions, as in Service Bus.
				continue
			}
			s, ok := v.(string)
			if !ok && v != nil {
				s = formatValue(v)
			}
			if err := m.SetProperty(System, canonical, s); err != nil {
				return err
			}
			continue
		}
		if s, ok := v.(string); ok {
			// A string sets typed properties by conversion.
			if old, ok := m.Property(User, st.prop.name); ok {
				switch normalize(old).(type) {
				case time.Time:
					t, err := time.Parse(time.RFC3339Nano, s)
					if err != nil {
						return evalErrorf("SET %s: %q is not a timestamp", st.prop.name, s)
					}
					v = t
				case GUID:
					g, ok := parseGUID(s)
					if !ok {
						return evalErrorf("SET %s: %q is not a GUID", st.prop.name, s)
					}
					v = g
				}
			}
		}
		if err := m.SetProperty(User, st.prop.name, v); err != nil {
			return err
		}
	}
	return nil
}
