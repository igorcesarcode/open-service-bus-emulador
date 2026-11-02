package filter

import (
	"fmt"
	"strings"
	"unicode"
)

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokIdent
	tokKeyword
	tokString
	tokInt
	tokFloat
	tokOp
	tokParam
)

type token struct {
	kind tokenKind
	// text is the identifier, the upper-case keyword, the unescaped
	// string, the number or the operator.
	text string
	// raw is the source text of a keyword.
	raw string
	pos int
}

func (t token) String() string {
	switch t.kind {
	case tokEOF:
		return "end of expression"
	case tokString:
		return fmt.Sprintf("'%s'", t.text)
	}
	return fmt.Sprintf("%q", t.text)
}

var keywords = map[string]bool{
	"AND": true, "OR": true, "NOT": true, "IN": true, "LIKE": true, "ESCAPE": true,
	"IS": true, "NULL": true, "EXISTS": true, "TRUE": true, "FALSE": true,
	"SET": true, "REMOVE": true,
}

// SyntaxError reports an expression that cannot be parsed.
type SyntaxError struct {
	Pos int
	Msg string
}

func (e *SyntaxError) Error() string {
	return fmt.Sprintf("filter: syntax error at position %d: %s", e.Pos, e.Msg)
}

func lex(src string) ([]token, error) {
	var out []token
	rs := []rune(src)
	for i := 0; i < len(rs); {
		r := rs[i]
		start := i
		switch {
		case unicode.IsSpace(r):
			i++
		case r == '@' && i+1 < len(rs) && (rs[i+1] == '_' || unicode.IsLetter(rs[i+1])):
			i++
			for i < len(rs) && (rs[i] == '_' || unicode.IsLetter(rs[i]) || unicode.IsDigit(rs[i])) {
				i++
			}
			out = append(out, token{kind: tokParam, text: string(rs[start:i]), pos: start})
		case unicode.IsLetter(r):
			for i < len(rs) && (rs[i] == '_' || unicode.IsLetter(rs[i]) || unicode.IsDigit(rs[i])) {
				i++
			}
			word := string(rs[start:i])
			if up := strings.ToUpper(word); keywords[up] {
				out = append(out, token{kind: tokKeyword, text: up, raw: word, pos: start})
			} else {
				out = append(out, token{kind: tokIdent, text: word, pos: start})
			}
		case r == '[' || r == '"':
			closer := ']'
			if r == '"' {
				closer = '"'
			}
			text, next, ok := delimited(rs, i+1, closer)
			if !ok {
				return nil, &SyntaxError{Pos: start, Msg: "unterminated identifier"}
			}
			out = append(out, token{kind: tokIdent, text: text, pos: start})
			i = next
		case r == '\'':
			text, next, ok := delimited(rs, i+1, '\'')
			if !ok {
				return nil, &SyntaxError{Pos: start, Msg: "unterminated string"}
			}
			out = append(out, token{kind: tokString, text: text, pos: start})
			i = next
		case unicode.IsDigit(r) || r == '.' && i+1 < len(rs) && unicode.IsDigit(rs[i+1]):
			kind := tokInt
			for i < len(rs) && unicode.IsDigit(rs[i]) {
				i++
			}
			if i < len(rs) && rs[i] == '.' {
				kind = tokFloat
				i++
				for i < len(rs) && unicode.IsDigit(rs[i]) {
					i++
				}
			}
			if i < len(rs) && (rs[i] == 'e' || rs[i] == 'E') {
				j := i + 1
				if j < len(rs) && (rs[j] == '+' || rs[j] == '-') {
					j++
				}
				if j < len(rs) && unicode.IsDigit(rs[j]) {
					kind = tokFloat
					i = j
					for i < len(rs) && unicode.IsDigit(rs[i]) {
						i++
					}
				}
			}
			out = append(out, token{kind: kind, text: string(rs[start:i]), pos: start})
		default:
			op := string(r)
			if i+1 < len(rs) {
				switch two := string(rs[i : i+2]); two {
				case "<>", "!=", "<=", ">=":
					op = two
				}
			}
			if !strings.Contains("=<>!+-*/%(),.;", op[:1]) || op == "!" {
				return nil, &SyntaxError{Pos: start, Msg: fmt.Sprintf("unexpected character %q", r)}
			}
			out = append(out, token{kind: tokOp, text: op, pos: start})
			i += len([]rune(op))
		}
	}
	return append(out, token{kind: tokEOF, pos: len(rs)}), nil
}

// delimited reads text up to closer, where a doubled closer stands for
// itself. It returns the index after the closer.
func delimited(rs []rune, i int, closer rune) (string, int, bool) {
	var b strings.Builder
	for i < len(rs) {
		if rs[i] == closer {
			if i+1 < len(rs) && rs[i+1] == closer {
				b.WriteRune(closer)
				i += 2
				continue
			}
			return b.String(), i + 1, true
		}
		b.WriteRune(rs[i])
		i++
	}
	return "", i, false
}
