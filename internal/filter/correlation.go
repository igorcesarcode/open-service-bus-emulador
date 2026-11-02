package filter

// CorrelationFilter matches messages whose properties equal every set
// field. String comparisons are case-sensitive.
type CorrelationFilter struct {
	CorrelationID    string `json:"correlationId,omitempty"`
	MessageID        string `json:"messageId,omitempty"`
	To               string `json:"to,omitempty"`
	ReplyTo          string `json:"replyTo,omitempty"`
	Subject          string `json:"subject,omitempty"`
	SessionID        string `json:"sessionId,omitempty"`
	ReplyToSessionID string `json:"replyToSessionId,omitempty"`
	ContentType      string `json:"contentType,omitempty"`
	Properties       Values `json:"properties,omitempty"`
}

// Match reports whether m matches every condition of f. A filter without
// conditions matches every message.
func (f *CorrelationFilter) Match(m Message) bool {
	for _, c := range []struct{ name, want string }{
		{"CorrelationId", f.CorrelationID},
		{"MessageId", f.MessageID},
		{"To", f.To},
		{"ReplyTo", f.ReplyTo},
		{"Label", f.Subject},
		{"SessionId", f.SessionID},
		{"ReplyToSessionId", f.ReplyToSessionID},
		{"ContentType", f.ContentType},
	} {
		if c.want == "" {
			continue
		}
		v, ok := m.Property(System, c.name)
		if s, isString := normalize(v).(string); !ok || !isString || s != c.want {
			return false
		}
	}
	for name, want := range f.Properties {
		v, ok := m.Property(User, name)
		if !ok || compare("=", normalize(v), normalize(want)) != true {
			return false
		}
	}
	return true
}
