package admin

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/duration"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
)

// Responses are written by hand: the JavaScript SDK reads namespace
// prefixes literally ("i:type", "d2p1:", "d6p1:"), which encoding/xml
// would rename.

const (
	atomNamespace    = "http://www.w3.org/2005/Atom"
	connectNamespace = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"
	countsNamespace  = "http://schemas.microsoft.com/netservices/2011/06/servicebus"
	xsdNamespace     = "http://www.w3.org/2001/XMLSchema"
	apiVersion       = "2024-05"
)

// xmlWriter builds an XML document.
type xmlWriter struct {
	b strings.Builder
}

func (w *xmlWriter) raw(s string) { w.b.WriteString(s) }

// escaper uses named entities only: the JavaScript SDK's parser does not
// decode numeric references such as &#39;, which encoding/xml writes.
var escaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&apos;")

func (w *xmlWriter) text(s string) { escaper.WriteString(&w.b, s) }

func (w *xmlWriter) attr(s string) { escaper.WriteString(&w.b, s) }

// el writes <name>value</name>.
func (w *xmlWriter) el(name, value string) {
	w.raw("<" + name + ">")
	w.text(value)
	w.raw("</" + name + ">")
}

func (w *xmlWriter) bool(name string, v bool)      { w.el(name, strconv.FormatBool(v)) }
func (w *xmlWriter) int(name string, v int64)      { w.el(name, strconv.FormatInt(v, 10)) }
func (w *xmlWriter) time(name string, t time.Time) { w.el(name, formatTime(t)) }

func (w *xmlWriter) duration(name string, d time.Duration) { w.el(name, duration.Format(d)) }

// optional writes an element only when the value is set.
func (w *xmlWriter) optional(name, value string) {
	if value != "" {
		w.el(name, value)
	}
}

// formatTime writes a time the way Service Bus does, with seven
// fractional digits. The zero time means never.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return "0001-01-01T00:00:00Z"
	}
	return t.UTC().Format("2006-01-02T15:04:05.0000000Z")
}

// entryLink is the absolute URL of path, the form the JavaScript SDK reads
// entity names from.
func entryLink(base, path string) string {
	return base + path + "?api-version=" + apiVersion
}

// writeEntry writes an ATOM entry around a description.
func writeEntry(w *xmlWriter, base, path, title string, published, updated time.Time, content func(*xmlWriter)) {
	link := entryLink(base, path)
	w.raw(`<entry xmlns="` + atomNamespace + `"><id>`)
	w.text(link)
	w.raw(`</id><title type="text">`)
	w.text(title)
	w.raw(`</title>`)
	w.time("published", published)
	w.time("updated", updated)
	w.raw(`<author><name>open-service-bus</name></author><link rel="self" href="`)
	w.attr(link)
	w.raw(`"/><content type="application/xml">`)
	content(w)
	w.raw(`</content></entry>`)
}

// feedPage is one page of a list.
type feedPage struct {
	title string
	base  string
	path  string
	skip  int
	top   int // 0 means everything
	total int
}

// writeFeed writes a feed with a self link and, when more entries remain,
// a next link. The JavaScript SDK finds the next link only among two or
// more links.
func writeFeed(w *xmlWriter, p feedPage, entries func(*xmlWriter)) {
	self := entryLink(p.base, p.path)
	w.raw(`<feed xmlns="` + atomNamespace + `"><title type="text">`)
	w.text(p.title)
	w.raw(`</title><id>`)
	w.text(self)
	w.raw(`</id>`)
	w.time("updated", time.Now())
	page := func(skip int) string {
		q := fmt.Sprintf("%s&$skip=%d", self, skip)
		if p.top > 0 {
			q += fmt.Sprintf("&$top=%d", p.top)
		}
		return q
	}
	w.raw(`<link rel="self" href="`)
	w.attr(page(p.skip))
	w.raw(`"/>`)
	if p.top > 0 && p.skip+p.top < p.total {
		w.raw(`<link rel="next" href="`)
		w.attr(page(p.skip + p.top))
		w.raw(`"/>`)
	}
	entries(w)
	w.raw(`</feed>`)
}

func openDescription(w *xmlWriter, name string) {
	w.raw(`<` + name + ` xmlns="` + connectNamespace + `" xmlns:i="` + xsiNamespace + `">`)
}

func writeCounts(w *xmlWriter, c broker.Counts) {
	w.raw(`<CountDetails xmlns:d2p1="` + countsNamespace + `">`)
	w.int("d2p1:ActiveMessageCount", c.Active)
	w.int("d2p1:DeadLetterMessageCount", c.DeadLetter)
	w.int("d2p1:ScheduledMessageCount", c.Scheduled)
	w.int("d2p1:TransferMessageCount", 0)
	w.int("d2p1:TransferDeadLetterMessageCount", c.TransferDeadLetter)
	w.raw(`</CountDetails>`)
}

func totalCount(c broker.Counts) int64 {
	return c.Active + c.DeadLetter + c.TransferDeadLetter + c.Scheduled + c.Deferred
}

func writeQueue(w *xmlWriter, e *broker.Entity, c broker.Counts, rules []auth.Policy) {
	p := e.Properties
	openDescription(w, "QueueDescription")
	w.duration("LockDuration", p.LockDuration)
	w.int("MaxSizeInMegabytes", p.MaxSizeInMegabytes)
	w.bool("RequiresDuplicateDetection", p.RequiresDuplicateDetection)
	w.bool("RequiresSession", p.RequiresSession)
	w.duration("DefaultMessageTimeToLive", p.DefaultMessageTimeToLive)
	w.bool("DeadLetteringOnMessageExpiration", p.DeadLetteringOnMessageExpiration)
	w.duration("DuplicateDetectionHistoryTimeWindow", p.DuplicateDetectionHistoryTimeWindow)
	w.int("MaxDeliveryCount", int64(p.MaxDeliveryCount))
	w.bool("EnableBatchedOperations", p.EnableBatchedOperations)
	w.int("SizeInBytes", c.SizeBytes)
	w.int("MessageCount", totalCount(c))
	w.bool("IsAnonymousAccessible", false)
	writeAuthorizationRules(w, rules)
	w.el("Status", string(p.Status))
	w.optional("ForwardTo", p.ForwardTo)
	w.time("CreatedAt", e.CreatedAt)
	w.time("UpdatedAt", e.UpdatedAt)
	w.time("AccessedAt", c.AccessedAt)
	w.bool("SupportOrdering", p.SupportOrdering)
	writeCounts(w, c)
	w.duration("AutoDeleteOnIdle", p.AutoDeleteOnIdle)
	w.bool("EnablePartitioning", p.EnablePartitioning)
	w.el("EntityAvailabilityStatus", "Available")
	w.bool("EnableExpress", p.EnableExpress)
	w.optional("ForwardDeadLetteredMessagesTo", p.ForwardDeadLetteredMessagesTo)
	w.optional("UserMetadata", p.UserMetadata)
	w.int("MaxMessageSizeInKilobytes", p.MaxMessageSizeInKilobytes)
	w.raw(`</QueueDescription>`)
}

// topicStats are the runtime figures of a topic.
type topicStats struct {
	counts            broker.Counts
	subscriptions     int
	sqlFilters        int
	correlationFilter int
}

func writeTopic(w *xmlWriter, e *broker.Entity, s topicStats, rules []auth.Policy) {
	p := e.Properties
	openDescription(w, "TopicDescription")
	w.duration("DefaultMessageTimeToLive", p.DefaultMessageTimeToLive)
	w.int("MaxSizeInMegabytes", p.MaxSizeInMegabytes)
	w.bool("RequiresDuplicateDetection", p.RequiresDuplicateDetection)
	w.duration("DuplicateDetectionHistoryTimeWindow", p.DuplicateDetectionHistoryTimeWindow)
	w.bool("EnableBatchedOperations", p.EnableBatchedOperations)
	w.int("SizeInBytes", s.counts.SizeBytes)
	w.bool("FilteringMessagesBeforePublishing", false)
	w.bool("IsAnonymousAccessible", false)
	writeAuthorizationRules(w, rules)
	w.el("Status", string(p.Status))
	w.time("CreatedAt", e.CreatedAt)
	w.time("UpdatedAt", e.UpdatedAt)
	w.time("AccessedAt", s.counts.AccessedAt)
	w.bool("SupportOrdering", p.SupportOrdering)
	writeCounts(w, s.counts)
	w.int("SubscriptionCount", int64(s.subscriptions))
	w.duration("AutoDeleteOnIdle", p.AutoDeleteOnIdle)
	w.bool("EnablePartitioning", p.EnablePartitioning)
	w.bool("IsExpress", p.EnableExpress)
	w.el("EntityAvailabilityStatus", "Available")
	w.bool("EnableSubscriptionPartitioning", false)
	w.bool("EnableExpress", p.EnableExpress)
	w.optional("UserMetadata", p.UserMetadata)
	w.int("MaxMessageSizeInKilobytes", p.MaxMessageSizeInKilobytes)
	w.int("SqlFilterCount", int64(s.sqlFilters))
	w.int("CorrelationFilterCount", int64(s.correlationFilter))
	w.raw(`</TopicDescription>`)
}

func writeSubscription(w *xmlWriter, e *broker.Entity, c broker.Counts) {
	p := e.Properties
	openDescription(w, "SubscriptionDescription")
	w.duration("LockDuration", p.LockDuration)
	w.bool("RequiresSession", p.RequiresSession)
	w.duration("DefaultMessageTimeToLive", p.DefaultMessageTimeToLive)
	w.bool("DeadLetteringOnMessageExpiration", p.DeadLetteringOnMessageExpiration)
	w.bool("DeadLetteringOnFilterEvaluationExceptions", p.DeadLetteringOnFilterEvaluationExceptions)
	w.int("MessageCount", totalCount(c))
	w.int("MaxDeliveryCount", int64(p.MaxDeliveryCount))
	w.bool("EnableBatchedOperations", p.EnableBatchedOperations)
	w.el("Status", string(p.Status))
	w.optional("ForwardTo", p.ForwardTo)
	w.time("CreatedAt", e.CreatedAt)
	w.time("UpdatedAt", e.UpdatedAt)
	w.time("AccessedAt", c.AccessedAt)
	writeCounts(w, c)
	w.optional("UserMetadata", p.UserMetadata)
	w.optional("ForwardDeadLetteredMessagesTo", p.ForwardDeadLetteredMessagesTo)
	w.duration("AutoDeleteOnIdle", p.AutoDeleteOnIdle)
	w.el("EntityAvailabilityStatus", "Available")
	w.raw(`</SubscriptionDescription>`)
}

func writeRule(w *xmlWriter, r broker.Rule) {
	openDescription(w, "RuleDescription")
	f := r.Filter
	switch f.Kind {
	case broker.FilterSQL:
		w.raw(`<Filter i:type="SqlFilter">`)
		w.el("SqlExpression", f.SQL)
		w.int("CompatibilityLevel", 20)
		writeValues(w, "Parameters", f.Parameters)
		w.raw(`</Filter>`)
	case broker.FilterCorrelation:
		c := f.Correlation
		w.raw(`<Filter i:type="CorrelationFilter">`)
		w.optional("CorrelationId", c.CorrelationID)
		w.optional("MessageId", c.MessageID)
		w.optional("To", c.To)
		w.optional("ReplyTo", c.ReplyTo)
		w.optional("Label", c.Subject)
		w.optional("SessionId", c.SessionID)
		w.optional("ReplyToSessionId", c.ReplyToSessionID)
		w.optional("ContentType", c.ContentType)
		writeValues(w, "Properties", c.Properties)
		w.raw(`</Filter>`)
	case broker.FilterFalse:
		// The JavaScript SDK tells filters apart by SqlExpression alone.
		w.raw(`<Filter i:type="FalseFilter"><SqlExpression>1=0</SqlExpression><CompatibilityLevel>20</CompatibilityLevel></Filter>`)
	default:
		w.raw(`<Filter i:type="TrueFilter"><SqlExpression>1=1</SqlExpression><CompatibilityLevel>20</CompatibilityLevel></Filter>`)
	}
	if r.Action != "" {
		w.raw(`<Action i:type="SqlRuleAction">`)
		w.el("SqlExpression", r.Action)
		w.int("CompatibilityLevel", 20)
		writeValues(w, "Parameters", r.ActionParameters)
		w.raw(`</Action>`)
	} else {
		// Both SDKs fail on a rule without an Action element.
		w.raw(`<Action i:type="EmptyRuleAction"/>`)
	}
	w.time("CreatedAt", r.CreatedAt)
	w.el("Name", r.Name)
	w.raw(`</RuleDescription>`)
}

// writeValues writes typed key/value pairs. The type prefix must be four
// characters long: the JavaScript SDK strips five characters to read it.
func writeValues(w *xmlWriter, name string, values filter.Values) {
	if len(values) == 0 {
		return
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sortStrings(keys)
	w.raw("<" + name + ">")
	for _, k := range keys {
		typ, text, ok := xsdValue(values[k])
		if !ok {
			continue
		}
		w.raw(`<KeyValueOfstringanyType>`)
		w.el("Key", k)
		w.raw(`<Value i:type="d6p1:` + typ + `" xmlns:d6p1="` + xsdNamespace + `">`)
		w.text(text)
		w.raw(`</Value></KeyValueOfstringanyType>`)
	}
	w.raw("</" + name + ">")
}

// xsdValue returns the XML schema type and text of a value, using only the
// types both SDKs accept.
func xsdValue(v any) (typ, text string, ok bool) {
	switch v := v.(type) {
	case string:
		return "string", v, true
	case bool:
		return "boolean", strconv.FormatBool(v), true
	case int64:
		return "int", strconv.FormatInt(v, 10), true
	case int32:
		return "int", strconv.FormatInt(int64(v), 10), true
	case int:
		return "int", strconv.Itoa(v), true
	case uint32:
		return "int", strconv.FormatUint(uint64(v), 10), true
	case uint64:
		if v <= math.MaxInt64 {
			return "int", strconv.FormatUint(v, 10), true
		}
		return "double", strconv.FormatFloat(float64(v), 'g', -1, 64), true
	case float64:
		return "double", strconv.FormatFloat(v, 'g', -1, 64), true
	case float32:
		return "double", strconv.FormatFloat(float64(v), 'g', -1, 64), true
	case time.Time:
		return "dateTime", v.UTC().Format(time.RFC3339Nano), true
	case time.Duration:
		return "duration", duration.Format(v), true
	case filter.GUID:
		return "string", v.String(), true
	}
	return "", "", false
}

func writeError(w *xmlWriter, code int, detail string) {
	w.raw(`<Error><Code>` + strconv.Itoa(code) + `</Code><Detail>`)
	w.text(detail)
	w.raw(`</Detail></Error>`)
}

// writeAuthorizationRules writes the shared access policies of an entity.
func writeAuthorizationRules(w *xmlWriter, rules []auth.Policy) {
	w.raw(`<AuthorizationRules>`)
	for _, p := range rules {
		w.raw(`<AuthorizationRule xmlns:p5="` + xsiNamespace + `" p5:type="SharedAccessAuthorizationRule">`)
		w.el("ClaimType", "SharedAccessKey")
		w.el("ClaimValue", "None")
		w.raw(`<Rights>`)
		for _, r := range p.Rights.Names() {
			w.el("AccessRights", r)
		}
		w.raw(`</Rights>`)
		w.el("KeyName", p.Name)
		w.el("PrimaryKey", p.PrimaryKey)
		w.el("SecondaryKey", p.SecondaryKey)
		w.raw(`</AuthorizationRule>`)
	}
	w.raw(`</AuthorizationRules>`)
}
