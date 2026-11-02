package filter

import (
	"cmp"
	"crypto/rand"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

// node is an expression. eval returns nil, unknown, bool, int64, float64,
// string, time.Time, GUID or opaque.
type node interface {
	eval(m Message) (any, error)
}

type literal struct{ v any }

func (n *literal) eval(Message) (any, error) { return n.v, nil }

type propRef struct {
	scope Scope
	name  string
}

func (n *propRef) eval(m Message) (any, error) { return lookup(m, n.scope, n.name) }

// lookup reads a property. A missing user property is unknown; a missing
// system property is null; an unknown system property is an error.
func lookup(m Message, scope Scope, name string) (any, error) {
	if scope == System {
		canonical, ok := SystemProperty(name)
		if !ok {
			return nil, evalErrorf("unknown system property sys.%s", name)
		}
		v, _ := m.Property(System, canonical)
		return normalize(v), nil
	}
	v, ok := m.Property(User, name)
	if !ok {
		return unknown, nil
	}
	return normalize(v), nil
}

// splitScope splits "sys.Name" or "user.Name", as accepted by property().
func splitScope(name string) (Scope, string) {
	if scope, rest, ok := strings.Cut(name, "."); ok {
		switch strings.ToLower(scope) {
		case "sys":
			return System, rest
		case "user":
			return User, rest
		}
	}
	return User, name
}

type propertyFuncNode struct{ arg node }

func (n *propertyFuncNode) eval(m Message) (any, error) {
	v, err := n.arg.eval(m)
	if err != nil {
		return nil, err
	}
	name, ok := v.(string)
	if !ok {
		return unknown, nil
	}
	scope, name := splitScope(name)
	return lookup(m, scope, name)
}

type newIDNode struct{}

func (n *newIDNode) eval(Message) (any, error) {
	var g GUID
	rand.Read(g[:])
	g[6] = g[6]&0x0f | 0x40
	g[8] = g[8]&0x3f | 0x80
	return g, nil
}

// isNull reports whether v is null or unknown.
func isNull(v any) bool { return v == nil || v == unknown }

// truth converts v to three-valued logic: true, false or unknown.
func truth(v any) any {
	if b, ok := v.(bool); ok {
		return b
	}
	return unknown
}

type andNode struct{ l, r node }

func (n *andNode) eval(m Message) (any, error) {
	l, err := n.l.eval(m)
	if err != nil {
		return nil, err
	}
	l = truth(l)
	if l == false {
		return false, nil
	}
	r, err := n.r.eval(m)
	if err != nil {
		return nil, err
	}
	r = truth(r)
	switch {
	case r == false:
		return false, nil
	case l == true && r == true:
		return true, nil
	}
	return unknown, nil
}

type orNode struct{ l, r node }

func (n *orNode) eval(m Message) (any, error) {
	l, err := n.l.eval(m)
	if err != nil {
		return nil, err
	}
	l = truth(l)
	if l == true {
		return true, nil
	}
	r, err := n.r.eval(m)
	if err != nil {
		return nil, err
	}
	r = truth(r)
	switch {
	case r == true:
		return true, nil
	case l == false && r == false:
		return false, nil
	}
	return unknown, nil
}

type notNode struct{ x node }

func (n *notNode) eval(m Message) (any, error) {
	v, err := n.x.eval(m)
	if err != nil {
		return nil, err
	}
	if b, ok := truth(v).(bool); ok {
		return !b, nil
	}
	return unknown, nil
}

type existsNode struct{ prop *propRef }

func (n *existsNode) eval(m Message) (any, error) {
	if n.prop.scope == System {
		canonical, ok := SystemProperty(n.prop.name)
		if !ok {
			return nil, evalErrorf("unknown system property sys.%s", n.prop.name)
		}
		_, set := m.Property(System, canonical)
		return set, nil
	}
	_, ok := m.Property(User, n.prop.name)
	return ok, nil
}

type isNullNode struct {
	x   node
	not bool
}

func (n *isNullNode) eval(m Message) (any, error) {
	v, err := n.x.eval(m)
	if err != nil {
		return nil, err
	}
	return isNull(v) != n.not, nil
}

type compareNode struct {
	op   string
	l, r node
}

func (n *compareNode) eval(m Message) (any, error) {
	l, err := n.l.eval(m)
	if err != nil {
		return nil, err
	}
	r, err := n.r.eval(m)
	if err != nil {
		return nil, err
	}
	return compare(n.op, l, r), nil
}

// compare applies a comparison operator. Null, unknown and operands of
// types that do not compare give unknown.
func compare(op string, l, r any) any {
	if isNull(l) || isNull(r) {
		return unknown
	}
	c, ok := order(l, r)
	if !ok {
		return unknown
	}
	if c == unordered {
		// Equality-only types.
		switch op {
		case "=":
			return equalValues(l, r)
		case "<>":
			return !equalValues(l, r)
		}
		return unknown
	}
	switch op {
	case "=":
		return c == 0
	case "<>":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	case ">=":
		return c >= 0
	}
	return unknown
}

const unordered = 2

// order compares two values of compatible types. It returns unordered for
// types that only support equality, and ok false for incompatible types.
func order(l, r any) (int, bool) {
	if lf, rf, ok := numbers(l, r); ok {
		switch {
		case lf < rf:
			return -1, true
		case lf > rf:
			return 1, true
		case lf == rf:
			return 0, true
		}
		return unordered, true // NaN
	}
	if li, ok := l.(int64); ok {
		if ri, ok := r.(int64); ok {
			switch {
			case li < ri:
				return -1, true
			case li > ri:
				return 1, true
			}
			return 0, true
		}
	}
	switch l := l.(type) {
	case time.Duration:
		if r, ok := r.(time.Duration); ok {
			return cmp.Compare(l, r), true
		}
	case time.Time:
		if r, ok := r.(time.Time); ok {
			return l.Compare(r), true
		}
	case string:
		if _, ok := r.(string); ok {
			return unordered, true
		}
	case bool:
		if _, ok := r.(bool); ok {
			return unordered, true
		}
	case GUID:
		if _, ok := r.(GUID); ok {
			return unordered, true
		}
	}
	return 0, false
}

// numbers returns both operands as float64 when at least one is a float
// and the other is numeric, following C# numeric promotion.
func numbers(l, r any) (float64, float64, bool) {
	lf, lok := l.(float64)
	rf, rok := r.(float64)
	if !lok && !rok {
		return 0, 0, false
	}
	if li, ok := l.(int64); ok {
		lf, lok = float64(li), true
	}
	if ri, ok := r.(int64); ok {
		rf, rok = float64(ri), true
	}
	return lf, rf, lok && rok
}

func equalValues(l, r any) bool {
	if c, ok := order(l, r); ok && c != unordered {
		return c == 0
	}
	return l == r
}

type inNode struct {
	x     node
	items []node
}

func (n *inNode) eval(m Message) (any, error) {
	v, err := n.x.eval(m)
	if err != nil {
		return nil, err
	}
	if isNull(v) {
		return unknown, nil
	}
	sawUnknown := false
	for _, item := range n.items {
		iv, err := item.eval(m)
		if err != nil {
			return nil, err
		}
		switch compare("=", v, iv) {
		case true:
			return true, nil
		case unknown:
			if isNull(iv) {
				sawUnknown = true
			}
		}
	}
	if sawUnknown {
		return unknown, nil
	}
	return false, nil
}

type likeNode struct {
	x, pattern, escape node
}

func (n *likeNode) eval(m Message) (any, error) {
	v, err := n.x.eval(m)
	if err != nil {
		return nil, err
	}
	pv, err := n.pattern.eval(m)
	if err != nil {
		return nil, err
	}
	var esc rune = -1
	if n.escape != nil {
		ev, err := n.escape.eval(m)
		if err != nil {
			return nil, err
		}
		if isNull(ev) {
			return unknown, nil
		}
		es, ok := ev.(string)
		if !ok || utf8.RuneCountInString(es) != 1 {
			return nil, evalErrorf("LIKE escape must be a string of one character")
		}
		esc, _ = utf8.DecodeRuneInString(es)
	}
	s, ok1 := v.(string)
	pattern, ok2 := pv.(string)
	if !ok1 || !ok2 {
		return unknown, nil
	}
	return like([]rune(s), []rune(pattern), esc), nil
}

// like matches s against a LIKE pattern with % and _ wildcards. Matching
// is case-sensitive, like the rest of the string comparisons.
func like(s, p []rune, esc rune) bool {
	for len(p) > 0 {
		c := p[0]
		switch {
		case c == esc && len(p) > 1:
			if len(s) == 0 || s[0] != p[1] {
				return false
			}
			s, p = s[1:], p[2:]
		case c == '%':
			for len(p) > 0 && p[0] == '%' {
				p = p[1:]
			}
			if len(p) == 0 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if like(s[i:], p, esc) {
					return true
				}
			}
			return false
		case c == '_':
			if len(s) == 0 {
				return false
			}
			s, p = s[1:], p[1:]
		default:
			if len(s) == 0 || s[0] != c {
				return false
			}
			s, p = s[1:], p[1:]
		}
	}
	return len(s) == 0
}

type unaryNode struct {
	op string
	x  node
}

func (n *unaryNode) eval(m Message) (any, error) {
	v, err := n.x.eval(m)
	if err != nil {
		return nil, err
	}
	switch v := v.(type) {
	case int64:
		if n.op == "-" {
			return -v, nil
		}
		return v, nil
	case float64:
		if n.op == "-" {
			return -v, nil
		}
		return v, nil
	}
	return unknown, nil
}

type arithNode struct {
	op   string
	l, r node
}

func (n *arithNode) eval(m Message) (any, error) {
	l, err := n.l.eval(m)
	if err != nil {
		return nil, err
	}
	r, err := n.r.eval(m)
	if err != nil {
		return nil, err
	}
	return arith(n.op, l, r)
}

// arith applies an arithmetic operator with C# semantics: integer
// operations wrap, integer division by zero is an error and + concatenates
// when either operand is a string.
func arith(op string, l, r any) (any, error) {
	if l == unknown || r == unknown {
		return unknown, nil
	}
	if op == "+" {
		_, ls := l.(string)
		_, rs := r.(string)
		if ls || rs {
			return formatValue(l) + formatValue(r), nil
		}
	}
	if l == nil || r == nil {
		return unknown, nil
	}
	if v, ok := timeArith(op, l, r); ok {
		return v, nil
	}
	li, lInt := l.(int64)
	ri, rInt := r.(int64)
	if lInt && rInt {
		switch op {
		case "+":
			return li + ri, nil
		case "-":
			return li - ri, nil
		case "*":
			return li * ri, nil
		case "/", "%":
			if ri == 0 {
				return nil, evalErrorf("division by zero")
			}
			if li == math.MinInt64 && ri == -1 {
				if op == "/" {
					return li, nil
				}
				return int64(0), nil
			}
			if op == "/" {
				return li / ri, nil
			}
			return li % ri, nil
		}
	}
	lf, rf, ok := numbers(l, r)
	if !ok {
		return unknown, nil
	}
	switch op {
	case "+":
		return lf + rf, nil
	case "-":
		return lf - rf, nil
	case "*":
		return lf * rf, nil
	case "/":
		return lf / rf, nil
	case "%":
		return math.Mod(lf, rf), nil
	}
	return unknown, nil
}

// timeArith applies the operators C# defines on DateTime and TimeSpan:
// DateTime - DateTime is a TimeSpan, DateTime +/- TimeSpan is a DateTime and
// TimeSpan +/- TimeSpan is a TimeSpan.
func timeArith(op string, l, r any) (any, bool) {
	switch l := l.(type) {
	case time.Time:
		switch r := r.(type) {
		case time.Time:
			if op == "-" {
				return l.Sub(r), true
			}
		case time.Duration:
			switch op {
			case "+":
				return l.Add(r), true
			case "-":
				return l.Add(-r), true
			}
		}
	case time.Duration:
		if r, ok := r.(time.Duration); ok {
			switch op {
			case "+":
				return l + r, true
			case "-":
				return l - r, true
			}
		}
		if r, ok := r.(time.Time); ok && op == "+" {
			return r.Add(l), true
		}
	}
	return nil, false
}
