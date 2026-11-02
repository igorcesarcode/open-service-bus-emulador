package filter

import (
	"fmt"
	"strconv"
	"strings"
)

type parser struct {
	toks []token
	i    int
}

func (p *parser) peek() token { return p.toks[p.i] }

func (p *parser) next() token {
	t := p.toks[p.i]
	if t.kind != tokEOF {
		p.i++
	}
	return t
}

func (p *parser) isKeyword(k string) bool {
	t := p.peek()
	return t.kind == tokKeyword && t.text == k
}

func (p *parser) isOp(op string) bool {
	t := p.peek()
	return t.kind == tokOp && t.text == op
}

func (p *parser) acceptKeyword(k string) bool {
	if p.isKeyword(k) {
		p.i++
		return true
	}
	return false
}

func (p *parser) acceptOp(op string) bool {
	if p.isOp(op) {
		p.i++
		return true
	}
	return false
}

func (p *parser) errorf(format string, args ...any) error {
	return &SyntaxError{Pos: p.peek().pos, Msg: fmt.Sprintf(format, args...)}
}

func (p *parser) expectOp(op string) error {
	if !p.acceptOp(op) {
		return p.errorf("expected %q, found %v", op, p.peek())
	}
	return nil
}

// parseOr parses a full predicate.
func (p *parser) parseOr() (node, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.acceptKeyword("OR") {
		r, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		l = &orNode{l, r}
	}
	return l, nil
}

func (p *parser) parseAnd() (node, error) {
	l, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.acceptKeyword("AND") {
		r, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		l = &andNode{l, r}
	}
	return l, nil
}

func (p *parser) parseNot() (node, error) {
	if p.acceptKeyword("NOT") {
		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &notNode{x}, nil
	}
	return p.parsePredicate()
}

func (p *parser) parsePredicate() (node, error) {
	if p.acceptKeyword("EXISTS") {
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		prop, err := p.parseProperty()
		if err != nil {
			return nil, err
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		return &existsNode{prop}, nil
	}
	l, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	t := p.peek()
	if t.kind == tokOp {
		switch t.text {
		case "=", "<>", "!=", "<", ">", "<=", ">=":
			p.next()
			r, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			op := t.text
			if op == "!=" {
				op = "<>"
			}
			return &compareNode{op: op, l: l, r: r}, nil
		}
	}
	if p.acceptKeyword("IS") {
		not := p.acceptKeyword("NOT")
		if !p.acceptKeyword("NULL") {
			return nil, p.errorf("expected NULL, found %v", p.peek())
		}
		return &isNullNode{x: l, not: not}, nil
	}
	not := false
	if p.isKeyword("NOT") {
		// NOT here can only be NOT IN or NOT LIKE.
		p.next()
		not = true
		if !p.isKeyword("IN") && !p.isKeyword("LIKE") {
			return nil, p.errorf("expected IN or LIKE after NOT, found %v", p.peek())
		}
	}
	switch {
	case p.acceptKeyword("IN"):
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		var items []node
		for {
			item, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			items = append(items, item)
			if !p.acceptOp(",") {
				break
			}
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		var n node = &inNode{x: l, items: items}
		if not {
			n = &notNode{n}
		}
		return n, nil
	case p.acceptKeyword("LIKE"):
		pattern, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		var escape node
		if p.acceptKeyword("ESCAPE") {
			if escape, err = p.parseAdditive(); err != nil {
				return nil, err
			}
		}
		var n node = &likeNode{x: l, pattern: pattern, escape: escape}
		if not {
			n = &notNode{n}
		}
		return n, nil
	}
	return l, nil
}

func (p *parser) parseAdditive() (node, error) {
	l, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}
	for p.isOp("+") || p.isOp("-") {
		op := p.next().text
		r, err := p.parseMultiplicative()
		if err != nil {
			return nil, err
		}
		l = &arithNode{op: op, l: l, r: r}
	}
	return l, nil
}

func (p *parser) parseMultiplicative() (node, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.isOp("*") || p.isOp("/") || p.isOp("%") {
		op := p.next().text
		r, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		l = &arithNode{op: op, l: l, r: r}
	}
	return l, nil
}

func (p *parser) parseUnary() (node, error) {
	if p.isOp("+") || p.isOp("-") {
		op := p.next().text
		x, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		if lit, ok := x.(*literal); ok && op == "-" {
			switch v := lit.v.(type) {
			case int64:
				return &literal{-v}, nil
			case float64:
				return &literal{-v}, nil
			}
		}
		return &unaryNode{op: op, x: x}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (node, error) {
	t := p.peek()
	switch t.kind {
	case tokString:
		p.next()
		return &literal{t.text}, nil
	case tokInt:
		p.next()
		v, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil {
			return nil, &SyntaxError{Pos: t.pos, Msg: fmt.Sprintf("integer %s out of range", t.text)}
		}
		return &literal{v}, nil
	case tokFloat:
		p.next()
		v, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return nil, &SyntaxError{Pos: t.pos, Msg: fmt.Sprintf("invalid number %s", t.text)}
		}
		return &literal{v}, nil
	case tokKeyword:
		switch t.text {
		case "TRUE":
			p.next()
			return &literal{true}, nil
		case "FALSE":
			p.next()
			return &literal{false}, nil
		case "NULL":
			p.next()
			return &literal{nil}, nil
		}
	case tokOp:
		if t.text == "(" {
			p.next()
			x, err := p.parseOr()
			if err != nil {
				return nil, err
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
			return x, nil
		}
	case tokParam:
		p.next()
		return &paramNode{name: t.text}, nil
	case tokIdent:
		if p.toks[p.i+1].kind == tokOp && p.toks[p.i+1].text == "(" {
			return p.parseFunction()
		}
		return p.parseProperty()
	}
	return nil, p.errorf("unexpected %v", t)
}

func (p *parser) parseFunction() (node, error) {
	name := p.next()
	p.next() // (
	switch strings.ToLower(name.text) {
	case "newid":
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		return &newIDNode{}, nil
	case "property", "p":
		arg, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		return &propertyFuncNode{arg}, nil
	}
	return nil, &SyntaxError{Pos: name.pos, Msg: fmt.Sprintf("unknown function %s", name.text)}
}

// parseProperty parses [scope.]name.
func (p *parser) parseProperty() (*propRef, error) {
	t := p.next()
	if t.kind != tokIdent {
		return nil, &SyntaxError{Pos: t.pos, Msg: fmt.Sprintf("expected a property name, found %v", t)}
	}
	scope := strings.ToLower(t.text)
	if (scope == "sys" || scope == "user") && p.isOp(".") {
		p.next()
		n := p.next()
		if n.kind != tokIdent && n.kind != tokKeyword {
			return nil, &SyntaxError{Pos: n.pos, Msg: fmt.Sprintf("expected a property name after %s., found %v", t.text, n)}
		}
		name := n.text
		if n.kind == tokKeyword {
			name = n.raw
		}
		if scope == "sys" {
			return &propRef{scope: System, name: name}, nil
		}
		return &propRef{scope: User, name: name}, nil
	}
	return &propRef{scope: User, name: t.text}, nil
}

// SQLFilter is a parsed SQL filter expression.
type SQLFilter struct {
	expr   node
	text   string
	params Values
}

// WithParameters returns a copy of f that resolves @name references to
// params.
func (f *SQLFilter) WithParameters(params Values) *SQLFilter {
	c := *f
	c.params = params
	return &c
}

// ParseSQLFilter parses a SQL filter expression. Only syntax is checked:
// like Service Bus, references to unknown system properties fail when the
// filter is evaluated.
func ParseSQLFilter(expr string) (*SQLFilter, error) {
	toks, err := lex(expr)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	if p.peek().kind == tokEOF {
		return nil, p.errorf("empty filter expression")
	}
	n, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind != tokEOF {
		return nil, p.errorf("unexpected %v", t)
	}
	return &SQLFilter{expr: n, text: expr}, nil
}

// String returns the expression text.
func (f *SQLFilter) String() string { return f.text }

// Match evaluates the filter against m. It returns an *EvalError when the
// expression cannot be evaluated, such as for an unknown system property.
func (f *SQLFilter) Match(m Message) (bool, error) {
	v, err := f.expr.eval(&paramMessage{Message: m, params: f.params})
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	return ok && b, nil
}
