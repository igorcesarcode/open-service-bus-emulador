package filter

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// testMessage is a message with case-insensitive property names.
type testMessage struct {
	sys  map[string]any
	user map[string]any
}

func (m *testMessage) Property(scope Scope, name string) (any, bool) {
	props := m.user
	if scope == System {
		props = m.sys
	}
	if v, ok := props[name]; ok {
		return v, true
	}
	for k, v := range props {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}
	return nil, false
}

func (m *testMessage) SetProperty(scope Scope, name string, v any) error {
	props := m.user
	if scope == System {
		props = m.sys
	}
	for k := range props {
		if strings.EqualFold(k, name) {
			delete(props, k)
		}
	}
	props[name] = v
	return nil
}

func (m *testMessage) RemoveProperty(name string) {
	for k := range m.user {
		if strings.EqualFold(k, name) {
			delete(m.user, k)
		}
	}
}

var created = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func newMessage() *testMessage {
	return &testMessage{
		sys: map[string]any{
			"MessageId":      "m-1",
			"Label":          "order.created",
			"ContentType":    "application/json",
			"SequenceNumber": int64(42),
		},
		user: map[string]any{
			"color":      "blue",
			"quantity":   int32(10),
			"count":      uint32(3),
			"price":      9.5,
			"isApproved": true,
			"event":      "completed",
			"createdAt":  created,
			"nothing":    nil,
			"blob":       []byte{1},
			"Path":       `C:\temp\a_b%`,
			"odd name":   "spaced",
		},
	}
}

func TestSQLFilter(t *testing.T) {
	tests := []struct {
		expr string
		want bool
	}{
		{"1=1", true},
		{"1=0", false},
		{"color = 'blue'", true},
		{"color = 'Blue'", false}, // Case-sensitive values.
		{"COLOR = 'blue'", true},  // Case-insensitive names.
		{"user.color = 'blue'", true},
		{"[color] = 'blue'", true},
		{`"color" = 'blue'`, true},
		{"[odd name] = 'spaced'", true},
		{"color <> 'red' AND color != 'green'", true},
		{"quantity = 10", true},
		{"quantity = 10.0", true},
		{"quantity > 5 AND quantity <= 10", true},
		{"count >= 3", true},
		{"price < 10", true},
		{"price * 2 = 19", true},
		{"quantity / 3 = 3", true}, // Integer division.
		{"quantity % 4 = 2", true},
		{"-quantity = -10", true},
		{"quantity + 1 * 2 = 12", true},
		{"(quantity + 1) * 2 = 22", true},
		{"color + '-' + 'x' = 'blue-x'", true},
		{"'n' + quantity = 'n10'", true},
		{"isApproved = TRUE", true},
		{"isApproved", true},
		{"NOT isApproved", false},
		{"event in ('completed', 'approved') AND isApproved = TRUE", true},
		{"event IN ('a', 'b')", false},
		{"event NOT IN ('a', 'b')", true},
		{"quantity IN (1, 10)", true},
		{"color LIKE 'bl%'", true},
		{"color LIKE '_lue'", true},
		{"color LIKE 'b%e'", true},
		{"color LIKE 'B%'", false},
		{"color NOT LIKE 'r%'", true},
		{`Path LIKE 'C:\temp\a!_b!%' ESCAPE '!'`, true},
		{`Path LIKE 'C:\temp\a!_c%' ESCAPE '!'`, false},
		{"missing IS NULL", true},
		{"nothing IS NULL", true},
		{"color IS NOT NULL", true},
		{"EXISTS(color)", true},
		{"EXISTS(missing)", false},
		{"EXISTS(nothing)", true},
		{"EXISTS(sys.MessageId)", true},
		{"EXISTS(sys.CorrelationId)", false},
		{"sys.Label = 'order.created'", true},
		{"sys.Subject = 'order.created'", true},
		{"sys.label = 'order.created'", true},
		{"sys.MessageId = 'm-1' AND sys.SequenceNumber = 42", true},
		{"sys.CorrelationId IS NULL", true},
		{"sys.CorrelationId = 'x'", false},
		{"property('color') = 'blue'", true},
		{"p('sys.Label') = 'order.created'", true},
		{"createdAt > createdAt - 1", false}, // Time arithmetic is unknown.
		{"createdAt = createdAt", true},
		{"newid() = newid()", false},
		{"newid() IS NOT NULL", true},
		{"1.5E1 = 15", true},
		{".5 = 0.5", true},
		{"'it''s' = 'it''s'", true},
		// Unknown and mismatched types.
		{"missing = 1", false},
		{"NOT (missing = 1)", false},
		{"missing = 1 OR color = 'blue'", true},
		{"missing = 1 AND color = 'blue'", false},
		{"NOT (missing = 1 AND color = 'red')", true},
		{"quantity = '10'", false},
		{"NOT (quantity = '10')", false},
		{"color > 'a'", false},
		{"blob = 1", false},
		{"nothing = nothing", false},
		{"missing IN ('a')", false},
		{"color IN ('a', missing)", false},
		{"NOT (color IN ('a', missing))", false},
		{"color IN ('blue', missing)", true},
		{"missing LIKE 'a%'", false},
		{"quantity LIKE '1%'", false},
		{"quantity + missing = 1", false},
		{"-color = 1", false},
	}
	for _, tt := range tests {
		f, err := ParseSQLFilter(tt.expr)
		if err != nil {
			t.Errorf("ParseSQLFilter(%q) error: %v", tt.expr, err)
			continue
		}
		got, err := f.Match(newMessage())
		if err != nil {
			t.Errorf("%q: Match() error: %v", tt.expr, err)
			continue
		}
		if got != tt.want {
			t.Errorf("%q: Match() = %v, want %v", tt.expr, got, tt.want)
		}
	}
}

func TestSQLFilterEvalErrors(t *testing.T) {
	for _, expr := range []string{
		"sys.Nope = 1",
		"EXISTS(sys.Nope)",
		"quantity / 0 = 1",
		"quantity % 0 = 1",
		"color LIKE 'b%' ESCAPE 'ab'",
		"missing = 1 OR sys.Nope = 1",
	} {
		f, err := ParseSQLFilter(expr)
		if err != nil {
			t.Errorf("ParseSQLFilter(%q) error: %v", expr, err)
			continue
		}
		if _, err := f.Match(newMessage()); !IsEvalError(err) {
			t.Errorf("%q: Match() error = %v, want an evaluation error", expr, err)
		}
	}
}

func TestParseSQLFilterErrors(t *testing.T) {
	for _, expr := range []string{
		"",
		"color =",
		"color = 'unterminated",
		"[unterminated = 1",
		"color = 1 AND",
		"(color = 1",
		"color = 1)",
		"color NOT = 1",
		"color IS 1",
		"color IN ()",
		"EXISTS(1)",
		"unknownfn(1) = 1",
		"color = 1 color = 2",
		"color ! 1",
		"color = 99999999999999999999",
		"and = 1",
		"color = #",
	} {
		if _, err := ParseSQLFilter(expr); err == nil {
			t.Errorf("ParseSQLFilter(%q) succeeded, want error", expr)
		}
	}
}

func TestSQLAction(t *testing.T) {
	tests := []struct {
		action   string
		wantUser map[string]any
		wantSys  map[string]any
	}{
		{
			action:   "SET quantity = quantity / 2",
			wantUser: map[string]any{"quantity": int64(5)},
		},
		{
			action:   "SET color = 'red'; SET priority = 1; REMOVE price;",
			wantUser: map[string]any{"color": "red", "priority": int64(1), "price": nil},
		},
		{
			action:  "SET sys.To = 'Entity' SET sys.Label = sys.Label + '.v2'",
			wantSys: map[string]any{"To": "Entity", "Label": "order.created.v2"},
		},
		{
			action:   "SET a = missing; SET b = quantity * price",
			wantUser: map[string]any{"a": nil, "b": 95.0},
		},
		{
			action:   "SET createdAt = '2027-01-01T00:00:00Z'",
			wantUser: map[string]any{"createdAt": time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)},
		},
		{
			action:   "SET [new prop] = TRUE; SET user.set = 'kw'",
			wantUser: map[string]any{"new prop": true, "set": "kw"},
		},
	}
	for _, tt := range tests {
		a, err := ParseSQLAction(tt.action)
		if err != nil {
			t.Errorf("ParseSQLAction(%q) error: %v", tt.action, err)
			continue
		}
		m := newMessage()
		if err := a.Apply(m); err != nil {
			t.Errorf("%q: Apply() error: %v", tt.action, err)
			continue
		}
		for k, want := range tt.wantUser {
			got, ok := m.user[k]
			if want == nil && !ok && k == "price" {
				continue
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("%q: user property %s mismatch (-want +got):\n%s", tt.action, k, diff)
			}
		}
		for k, want := range tt.wantSys {
			if diff := cmp.Diff(want, m.sys[k]); diff != "" {
				t.Errorf("%q: system property %s mismatch (-want +got):\n%s", tt.action, k, diff)
			}
		}
	}
}

func TestSQLActionErrors(t *testing.T) {
	for _, tt := range []struct {
		action  string
		parsing bool
	}{
		{"", true},
		{"SET", true},
		{"SET a", true},
		{"SET a = ", true},
		{"UPDATE a = 1", true},
		{"SET a = 1 REMOVE", true},
		{"REMOVE sys.To", false},
		{"SET sys.SequenceNumber = 1", false},
		{"SET sys.Nope = 'x'", false},
		{"SET x = quantity / 0", false},
		{"SET createdAt = 'not a time'", false},
		{"SET x = blob", false},
	} {
		a, err := ParseSQLAction(tt.action)
		if tt.parsing {
			if err == nil {
				t.Errorf("ParseSQLAction(%q) succeeded, want error", tt.action)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseSQLAction(%q) error: %v", tt.action, err)
			continue
		}
		if err := a.Apply(newMessage()); !IsEvalError(err) {
			t.Errorf("%q: Apply() error = %v, want an evaluation error", tt.action, err)
		}
	}
}

func TestCorrelationFilter(t *testing.T) {
	tests := []struct {
		name string
		f    CorrelationFilter
		want bool
	}{
		{"empty", CorrelationFilter{}, true},
		{"subject", CorrelationFilter{Subject: "order.created"}, true},
		{"subject case", CorrelationFilter{Subject: "Order.Created"}, false},
		{"content type and message id", CorrelationFilter{ContentType: "application/json", MessageID: "m-1"}, true},
		{"unset system property", CorrelationFilter{CorrelationID: "c"}, false},
		{"property", CorrelationFilter{Properties: map[string]any{"color": "blue", "quantity": 10.0}}, true},
		{"property name case", CorrelationFilter{Properties: map[string]any{"Color": "blue"}}, true},
		{"property mismatch", CorrelationFilter{Subject: "order.created", Properties: map[string]any{"color": "red"}}, false},
		{"missing property", CorrelationFilter{Properties: map[string]any{"missing": "x"}}, false},
	}
	for _, tt := range tests {
		if got := tt.f.Match(newMessage()); got != tt.want {
			t.Errorf("%s: Match() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// TestPrivateModuleFilters parses every sql_filter of the private-module
// Terraform file and evaluates it against messages built from its own
// literals, so each expression is exercised with matching and missing
// properties.
func TestPrivateModuleFilters(t *testing.T) {
	data, err := os.ReadFile("../../test/integration/testdata/private-module/main.tf")
	if err != nil {
		t.Skipf("private-module fixture not available: %v", err)
	}
	re := regexp.MustCompile(`sql_filter\s*=\s*"((?:[^"\\]|\\.)*)"`)
	matches := re.FindAllStringSubmatch(string(data), -1)
	if len(matches) < 100 {
		t.Fatalf("found %d sql_filter expressions, want the fixture corpus", len(matches))
	}
	assign := regexp.MustCompile(`(\w+)\s*(?:=|in)\s*\(?\s*('(?:[^']|'')*'|TRUE|FALSE|\d+)`)
	for _, m := range matches {
		expr := strings.ReplaceAll(m[1], `\"`, `"`)
		f, err := ParseSQLFilter(expr)
		if err != nil {
			t.Errorf("ParseSQLFilter(%q) error: %v", expr, err)
			continue
		}
		// A message with none of the properties never matches, unless
		// the filter is constant.
		empty := &testMessage{sys: map[string]any{}, user: map[string]any{}}
		got, err := f.Match(empty)
		if err != nil {
			t.Errorf("%q: Match(empty) error: %v", expr, err)
		}
		if got && expr != "1=1" {
			t.Errorf("%q: Match(empty) = true, want false", expr)
		}
		// A message with the first value of every condition matches.
		msg := &testMessage{sys: map[string]any{}, user: map[string]any{}}
		for _, a := range assign.FindAllStringSubmatch(expr, -1) {
			name, lit := a[1], a[2]
			if _, set := msg.user[name]; set {
				continue
			}
			switch {
			case lit == "TRUE":
				msg.user[name] = true
			case lit == "FALSE":
				msg.user[name] = false
			case strings.HasPrefix(lit, "'"):
				msg.user[name] = strings.ReplaceAll(lit[1:len(lit)-1], "''", "'")
			default:
				msg.user[name] = int64(len(lit)) // Numbers only appear in 1=1.
			}
		}
		got, err = f.Match(msg)
		if err != nil {
			t.Errorf("%q: Match(%v) error: %v", expr, msg.user, err)
		} else if !got {
			t.Errorf("%q: Match(%v) = false, want true", expr, msg.user)
		}
	}
}

func TestParameters(t *testing.T) {
	params := Values{"@c": "blue", "@n": int64(5), "@f": 1.5}
	for expr, want := range map[string]bool{
		"color = @c":                   true,
		"color = @C AND quantity > @n": true,
		"price > @f":                   true,
		"color = @missing":             false,
	} {
		f, err := ParseSQLFilter(expr)
		if err != nil {
			t.Fatalf("ParseSQLFilter(%q) error: %v", expr, err)
		}
		got, err := f.WithParameters(params).Match(newMessage())
		if expr == "color = @missing" {
			if !IsEvalError(err) {
				t.Errorf("%q: Match() error = %v, want an undefined parameter error", expr, err)
			}
			continue
		}
		if err != nil || got != want {
			t.Errorf("%q: Match() = %v, %v; want %v", expr, got, err, want)
		}
	}
	a, err := ParseSQLAction("SET x = @c + 'x'")
	if err != nil {
		t.Fatal(err)
	}
	m := newMessage()
	if err := a.WithParameters(params).Apply(m); err != nil || m.user["x"] != "bluex" {
		t.Errorf("Apply() = %v, x = %v; want bluex", err, m.user["x"])
	}
}

func TestValuesJSON(t *testing.T) {
	in := Values{
		"s": "x", "i": int64(7), "u": uint32(9), "d": 7.0, "b": true, "n": nil,
		"t": time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC), "g": GUID{1, 2, 3}, "span": 90 * time.Minute,
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}
	var out Values
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}
	want := Values{
		"s": "x", "i": int64(7), "u": int64(9), "d": 7.0, "b": true, "n": nil,
		"t": time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC), "g": GUID{1, 2, 3}, "span": 90 * time.Minute,
	}
	if diff := cmp.Diff(want, out); diff != "" {
		t.Errorf("round trip mismatch (-want +got):\n%s", diff)
	}
}

// TestDocumentedFilters runs the examples of the Service Bus pages "SQL
// filter syntax" and "Service Bus filter examples": a filter, the user
// properties of a message and whether it matches.
func TestDocumentedFilters(t *testing.T) {
	type props = map[string]any
	tests := []struct {
		name   string
		expr   string
		params Values
		props  props
		sys    props
		want   bool
	}{
		// LIKE.
		{"prefix", `user.Code LIKE 'order-%'`, nil, props{"Code": "order-"}, nil, true},
		{"prefix 2", `user.Code LIKE 'order-%'`, nil, props{"Code": "order-123"}, nil, true},
		{"prefix no", `user.Code LIKE 'order-%'`, nil, props{"Code": "preorder-123"}, nil, false},
		{"suffix", `user.Code LIKE '%-complete'`, nil, props{"Code": "-complete"}, nil, true},
		{"suffix no", `user.Code LIKE '%-complete'`, nil, props{"Code": "order-complete-extra"}, nil, false},
		{"contains", `user.Code LIKE '%order%'`, nil, props{"Code": "preorder-123"}, nil, true},
		{"contains no", `user.Code LIKE '%order%'`, nil, props{"Code": "invoice-123"}, nil, false},
		{"one char", `user.Code LIKE 'item-_'`, nil, props{"Code": "item-7"}, nil, true},
		{"one char none", `user.Code LIKE 'item-_'`, nil, props{"Code": "item-"}, nil, false},
		{"one char two", `user.Code LIKE 'item-_'`, nil, props{"Code": "item-AB"}, nil, false},
		{"all, empty", `user.Code LIKE '%'`, nil, props{"Code": ""}, nil, true},
		{"all, absent", `user.Code LIKE '%'`, nil, props{}, nil, false},
		{"not like", `user.Code NOT LIKE 'test-%'`, nil, props{"Code": "order-123"}, nil, true},
		{"not like no", `user.Code NOT LIKE 'test-%'`, nil, props{"Code": "test-"}, nil, false},
		{"not like absent", `user.Code NOT LIKE 'test-%'`, nil, props{}, nil, false},
		{"sys like", `sys.label LIKE '%bus%'`, nil, nil, props{"Label": "service bus"}, true},
		{"sys correlation like", `sys.correlationid like 'abc-%'`, nil, nil, props{"CorrelationId": "abc-1"}, true},
		// Literal wildcards and ESCAPE.
		{"equals, percent literal", `user.Code = 'discount%_code'`, nil, props{"Code": "discount%_code"}, nil, true},
		{"equals, percent not wild", `user.Code = 'discount%_code'`, nil, props{"Code": "discount10_code"}, nil, false},
		{"escaped percent", `user.Code LIKE '%!%%' ESCAPE '!'`, nil, props{"Code": "discount%code"}, nil, true},
		{"escaped percent alone", `user.Code LIKE '%!%%' ESCAPE '!'`, nil, props{"Code": "%"}, nil, true},
		{"escaped percent no", `user.Code LIKE '%!%%' ESCAPE '!'`, nil, props{"Code": "discount10code"}, nil, false},
		{"escaped underscore", `user.Code LIKE 'order!_%' ESCAPE '!'`, nil, props{"Code": "order_123"}, nil, true},
		{"escaped underscore no", `user.Code LIKE 'order!_%' ESCAPE '!'`, nil, props{"Code": "orderX123"}, nil, false},
		{"escaped escape", `user.Code LIKE 'folder!!%' ESCAPE '!'`, nil, props{"Code": "folder!archive"}, nil, true},
		{"escaped escape no", `user.Code LIKE 'folder!!%' ESCAPE '!'`, nil, props{"Code": "folderarchive"}, nil, false},
		{"backslash escape", `property LIKE 'ABC\%' ESCAPE '\'`, nil, props{"property": "ABC%"}, nil, true},
		{"backslash escape no", `property LIKE 'ABC\%' ESCAPE '\'`, nil, props{"property": "ABCD"}, nil, false},
		// Property names.
		{"delimited", `[Order Type] = 'priority'`, nil, props{"Order Type": "priority"}, nil, true},
		{"user delimited", `user.[HR-EmployeeID] = 'E123'`, nil, props{"HR-EmployeeID": "E123"}, nil, true},
		{"uri name", `[http://schemas.microsoft.com/xrm/2011/Claims/EntityLogicalName] = 'account'`, nil,
			props{"http://schemas.microsoft.com/xrm/2011/Claims/EntityLogicalName": "account"}, nil, true},
		{"closing bracket", `[Order]]Type] = 'priority'`, nil, props{"Order]Type": "priority"}, nil, true},
		{"quoted name", `"http://schemas.microsoft.com/xrm/2011/Claims/EntityLogicalName" = 'account'`, nil,
			props{"http://schemas.microsoft.com/xrm/2011/Claims/EntityLogicalName": "account"}, nil, true},
		{"name case", `MESSAGEPROPERTY = 'A'`, nil, props{"MessageProperty": "A"}, nil, true},
		// String values.
		{"quote", `user.CustomerName = 'O''Brien'`, nil, props{"CustomerName": "O'Brien"}, nil, true},
		{"quote no", `user.CustomerName = 'O''Brien'`, nil, props{"CustomerName": "OBrien"}, nil, false},
		{"punctuation", `user.CustomerName = 'North & South - Retail/Online'`, nil, props{"CustomerName": "North & South - Retail/Online"}, nil, true},
		// Combined and numeric.
		{"combined", `(user.Code LIKE 'order-%' OR user.Code LIKE 'return-%') AND user.Quantity >= 10`, nil, props{"Code": "return-123", "Quantity": int32(15)}, nil, true},
		{"combined no", `(user.Code LIKE 'order-%' OR user.Code LIKE 'return-%') AND user.Quantity >= 10`, nil, props{"Code": "order-123", "Quantity": int32(9)}, nil, false},
		{"combined other", `(user.Code LIKE 'order-%' OR user.Code LIKE 'return-%') AND user.Quantity >= 10`, nil, props{"Code": "invoice-123", "Quantity": int32(15)}, nil, false},
		{"int equals", `MessageProperty = 1`, nil, props{"MessageProperty": int64(1)}, nil, true},
		{"int, decimal compare", `MessageProperty > 2.08`, nil, props{"MessageProperty": int32(3)}, nil, true},
		{"two properties", `MessageProperty = 1 AND MessageProperty2 = 3`, nil, props{"MessageProperty": int8(1), "MessageProperty2": uint16(3)}, nil, true},
		{"scientific", `p = 101.5E5`, nil, props{"p": 10150000.0}, nil, true},
		{"scientific small", `p = 0.5E-2`, nil, props{"p": 0.005}, nil, true},
		{"color and quantity", `color='blue' AND quantity=10`, nil, props{"color": "blue", "quantity": int32(10)}, nil, true},
		{"in", `StoreId IN('Store1', 'Store2', 'Store3')`, nil, props{"StoreId": "Store2"}, nil, true},
		{"sys.To in", `sys.To IN ('Store5','Store6') OR StoreId = 'Store8'`, nil, props{"StoreId": "Store8"}, props{"To": "Store9"}, true},
		{"not in with unknown", `sys.To NOT IN ('Store1') OR StoreId NOT IN ('Store1')`, nil, props{}, props{"To": "Store1"}, false},
		// DateTime and TimeSpan.
		{"datetime param", `DateTimeMp < @dtParam`, Values{"@dtParam": created}, props{"DateTimeMp": created.Add(-time.Hour)}, nil, true},
		{"datetime param no", `DateTimeMp > @dtParam`, Values{"@dtParam": created}, props{"DateTimeMp": created.Add(-time.Hour)}, nil, false},
		{"timespan difference", `(DateTimeMp2-DateTimeMp1) <= @timespan`, Values{"@timespan": time.Hour},
			props{"DateTimeMp1": created, "DateTimeMp2": created.Add(30 * time.Minute)}, nil, true},
		{"timespan difference no", `DateTimeMp2-DateTimeMp1 <= @timespan`, Values{"@timespan": time.Hour},
			props{"DateTimeMp1": created, "DateTimeMp2": created.Add(2 * time.Hour)}, nil, false},
		{"datetime plus timespan", `DateTimeMp + @timespan > @dtParam`, Values{"@timespan": time.Hour, "@dtParam": created},
			props{"DateTimeMp": created}, nil, true},
		{"timespan sum", `@a + @b = @c`, Values{"@a": time.Minute, "@b": time.Second, "@c": 61 * time.Second}, nil, nil, true},
		{"sys.TimeToLive", `sys.TimeToLive < @ttl`, Values{"@ttl": time.Hour}, nil, props{"TimeToLive": 5 * time.Minute}, true},
		{"timespan with number", `@a > 1`, Values{"@a": time.Minute}, nil, nil, false},
	}
	for _, tt := range tests {
		f, err := ParseSQLFilter(tt.expr)
		if err != nil {
			t.Errorf("%s: ParseSQLFilter(%q) error: %v", tt.name, tt.expr, err)
			continue
		}
		m := &testMessage{sys: props{}, user: props{}}
		for k, v := range tt.props {
			m.user[k] = v
		}
		for k, v := range tt.sys {
			m.sys[k] = v
		}
		got, err := f.WithParameters(tt.params).Match(m)
		if err != nil {
			t.Errorf("%s: %q: Match() error: %v", tt.name, tt.expr, err)
			continue
		}
		if got != tt.want {
			t.Errorf("%s: %q: Match() = %v, want %v", tt.name, tt.expr, got, tt.want)
		}
	}
}

func TestTimeSpanFormat(t *testing.T) {
	for d, want := range map[time.Duration]string{
		0:                                 "00:00:00",
		90 * time.Second:                  "00:01:30",
		26*time.Hour + 5*time.Minute:      "1.02:05:00",
		-time.Minute:                      "-00:01:00",
		1500 * time.Millisecond:           "00:00:01.5",
		time.Second + 100*time.Nanosecond: "00:00:01.0000001",
	} {
		if got := formatTimeSpan(d); got != want {
			t.Errorf("formatTimeSpan(%v) = %q, want %q", d, got, want)
		}
	}
}

// TestDocumentedAction runs the action of the filter examples page.
func TestDocumentedAction(t *testing.T) {
	a, err := ParseSQLAction("SET quantity = quantity / 2; REMOVE priority;SET sys.CorrelationId = 'low';")
	if err != nil {
		t.Fatal(err)
	}
	m := &testMessage{sys: map[string]any{}, user: map[string]any{"color": "red", "quantity": int64(5), "priority": "high"}}
	if err := a.Apply(m); err != nil {
		t.Fatal(err)
	}
	if m.user["quantity"] != int64(2) {
		t.Errorf("quantity = %v, want 2 (integer division)", m.user["quantity"])
	}
	if _, ok := m.user["priority"]; ok {
		t.Error("priority was not removed")
	}
	if m.sys["CorrelationId"] != "low" {
		t.Errorf("CorrelationId = %v, want low", m.sys["CorrelationId"])
	}
}
