package config

import (
	"encoding/json"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

// Snapshot reads the entities, rules, access policies, role assignments,
// resource settings, Geo-DR pairings and Geo-Replication of every namespace
// of s, as a Config that starts an emulator with the same state (without
// the messages). A pairing that failed over or broke is not exported.
func Snapshot(s *server.Server) *Config {
	var all []*Config
	for _, n := range s.Namespaces() {
		all = append(all, snapshotNamespace(n))
	}
	out := all[0]
	if len(all) > 1 {
		out = &Config{Namespaces: all}
	}
	for _, g := range s.GeoDRAliases() {
		if g.Paired {
			out.GeoDR = append(out.GeoDR, server.GeoDRConfig{Alias: g.Alias, Primary: g.Primary, Secondary: g.Secondary})
		}
	}
	return out
}

func snapshotNamespace(n *server.Namespace) *Config {
	b, a := n.Broker(), n.Authority()
	c := &Config{Name: n.Name(), LocalAuthDisabled: !a.LocalAuthEnabled()}
	meta := b.Meta()
	c.Tags, c.Locks, c.Encryption = meta.Tags, meta.Locks, meta.Encryption
	if st := n.GeoReplicationStatus(); st.Enabled {
		c.GeoReplication = &server.GeoReplication{
			Synchronous: st.Synchronous, MaxLag: st.MaxLag, Delay: st.Delay,
			PrimaryRegion: st.PrimaryRegion, SecondaryRegion: st.SecondaryRegion,
		}
	}
	for _, p := range a.Policies("", true) {
		if p.Scope == "" && p.Name == auth.RootPolicyName {
			continue // Every namespace has it.
		}
		c.Policies = append(c.Policies, p)
	}
	for _, x := range a.Assignments() {
		c.Assignments = append(c.Assignments, server.Assignment{Principal: x.Principal, Role: x.Role, Scope: x.Scope})
	}
	for _, e := range b.Entities(broker.KindQueue, "") {
		c.Queues = append(c.Queues, server.Queue{Name: e.Name, Properties: e.Properties})
	}
	for _, t := range b.Entities(broker.KindTopic, "") {
		topic := server.Topic{Name: t.Name, Properties: t.Properties}
		for _, e := range b.Entities(broker.KindSubscription, t.Path) {
			sub := server.Subscription{Name: e.Name, Properties: e.Properties}
			if rules, err := b.Rules(e.Path); err == nil && !onlyDefaultRule(rules) {
				sub.Rules = rules
			}
			topic.Subscriptions = append(topic.Subscriptions, sub)
		}
		c.Topics = append(c.Topics, topic)
	}
	return c
}

// onlyDefaultRule reports whether rules is what a new subscription has.
func onlyDefaultRule(rules []broker.Rule) bool {
	return len(rules) == 1 && rules[0].Name == broker.DefaultRuleName &&
		rules[0].Filter.Kind == broker.FilterTrue && rules[0].Action == ""
}

// Marshal writes c as a Config.json document, in the format LoadJSON reads.
// A Config of several namespaces writes each as one.
func Marshal(c *Config) ([]byte, error) {
	var doc outDocument
	for _, ns := range c.entries() {
		doc.UserConfig.Namespaces = append(doc.UserConfig.Namespaces, exportNamespace(ns))
	}
	if doc.UserConfig.Namespaces == nil {
		doc.UserConfig.Namespaces = []outNamespace{}
	}
	for _, g := range c.GeoDR {
		doc.UserConfig.GeoDR = append(doc.UserConfig.GeoDR, jsonGeoDR(g))
	}
	return json.MarshalIndent(doc, "", "  ")
}

type outDocument struct {
	UserConfig struct {
		Namespaces []outNamespace
		GeoDR      []jsonGeoDR `json:",omitempty"`
	}
}

type outNamespace struct {
	Name               string
	AuthorizationRules []jsonPolicy       `json:",omitempty"`
	RoleAssignments    []jsonAssignment   `json:",omitempty"`
	LocalAuthEnabled   *bool              `json:",omitempty"`
	Tags               map[string]string  `json:",omitempty"`
	Locks              []jsonLock         `json:",omitempty"`
	Encryption         *jsonEncryption    `json:",omitempty"`
	GeoReplication     *outGeoReplication `json:",omitempty"`
	Queues             []outQueue
	Topics             []outTopic
}

type outGeoReplication struct {
	Mode                         string
	MaxReplicationLagSeconds     float64
	ReplicationDelayMilliseconds int64
	PrimaryRegion                string `json:",omitempty"`
	SecondaryRegion              string `json:",omitempty"`
}

type outQueue struct {
	Name               string
	Properties         jsonProperties
	AuthorizationRules []jsonPolicy `json:",omitempty"`
}

type outTopic struct {
	Name               string
	Properties         jsonProperties
	AuthorizationRules []jsonPolicy `json:",omitempty"`
	Subscriptions      []outSubscription
}

type outSubscription struct {
	Name       string
	Properties jsonProperties
	Rules      []outRule `json:",omitempty"`
}

type outRule struct {
	Name       string
	Properties outRuleProperties
}

type outRuleProperties struct {
	FilterType        string
	SQLFilter         *outSQL        `json:"SqlFilter,omitempty"`
	CorrelationFilter map[string]any `json:",omitempty"`
	Action            *outSQL        `json:",omitempty"`
}

type outSQL struct {
	SQLExpression string `json:"SqlExpression"`
}

func exportNamespace(c *Config) outNamespace {
	ns := outNamespace{
		Name: c.Name, Tags: c.Tags, Encryption: nil,
		Queues: []outQueue{}, Topics: []outTopic{},
	}
	if c.LocalAuthDisabled {
		off := false
		ns.LocalAuthEnabled = &off
	}
	if e := c.Encryption; e != nil {
		ns.Encryption = &jsonEncryption{KeyVaultKeyIDs: e.KeyVaultKeyIDs, Identity: e.Identity, InfrastructureEncryption: e.InfrastructureEncryption}
	}
	for _, l := range c.Locks {
		ns.Locks = append(ns.Locks, jsonLock{Name: l.Name, Level: string(l.Level), Scope: l.Scope, Notes: l.Notes})
	}
	if g := c.GeoReplication; g != nil {
		mode := "Asynchronous"
		if g.Synchronous {
			mode = "Synchronous"
		}
		ns.GeoReplication = &outGeoReplication{
			Mode: mode, MaxReplicationLagSeconds: g.MaxLag.Seconds(), ReplicationDelayMilliseconds: g.Delay.Milliseconds(),
			PrimaryRegion: g.PrimaryRegion, SecondaryRegion: g.SecondaryRegion,
		}
	}
	scoped := map[string][]jsonPolicy{}
	for _, p := range c.Policies {
		jp := jsonPolicy{Name: p.Name, Rights: p.Rights.Names(), PrimaryKey: p.PrimaryKey, SecondaryKey: p.SecondaryKey}
		if p.Scope == "" {
			ns.AuthorizationRules = append(ns.AuthorizationRules, jp)
		} else {
			scoped[p.Scope] = append(scoped[p.Scope], jp)
		}
	}
	for _, a := range c.Assignments {
		ns.RoleAssignments = append(ns.RoleAssignments, jsonAssignment{Principal: a.Principal, Role: string(a.Role), Scope: a.Scope})
	}
	for _, q := range c.Queues {
		ns.Queues = append(ns.Queues, outQueue{Name: q.Name, Properties: exportProperties(broker.KindQueue, q.Properties), AuthorizationRules: scoped[broker.NormalizePath(q.Name)]})
	}
	for _, t := range c.Topics {
		ot := outTopic{Name: t.Name, Properties: exportProperties(broker.KindTopic, t.Properties), AuthorizationRules: scoped[broker.NormalizePath(t.Name)], Subscriptions: []outSubscription{}}
		for _, s := range t.Subscriptions {
			os := outSubscription{Name: s.Name, Properties: exportProperties(broker.KindSubscription, s.Properties)}
			if s.Rules != nil {
				os.Rules = []outRule{}
				for _, r := range s.Rules {
					os.Rules = append(os.Rules, exportRule(r))
				}
			}
			ot.Subscriptions = append(ot.Subscriptions, os)
		}
		ns.Topics = append(ns.Topics, ot)
	}
	return ns
}

func exportRule(r broker.Rule) outRule {
	p := outRuleProperties{FilterType: string(r.Filter.Kind)}
	switch r.Filter.Kind {
	case broker.FilterSQL:
		p.SQLFilter = &outSQL{r.Filter.SQL}
	case broker.FilterCorrelation:
		c := map[string]any{}
		f := r.Filter.Correlation
		for k, v := range map[string]string{
			"CorrelationId": f.CorrelationID, "MessageId": f.MessageID, "To": f.To, "ReplyTo": f.ReplyTo,
			"Label": f.Subject, "SessionId": f.SessionID, "ReplyToSessionId": f.ReplyToSessionID, "ContentType": f.ContentType,
		} {
			if v != "" {
				c[k] = v
			}
		}
		if len(f.Properties) > 0 {
			c["Properties"] = map[string]any(f.Properties)
		}
		p.CorrelationFilter = c
	}
	if r.Action != "" {
		p.Action = &outSQL{r.Action}
	}
	return outRule{Name: r.Name, Properties: p}
}

// exportProperties keeps the properties that differ from the defaults of
// kind, which is all that a Config.json needs.
func exportProperties(kind broker.EntityKind, p broker.Properties) jsonProperties {
	if p == (broker.Properties{}) {
		return jsonProperties{}
	}
	d := broker.DefaultProperties(kind)
	var j jsonProperties
	dur := func(dst **isoDuration, v, def time.Duration) {
		if v != def {
			x := isoDuration(v)
			*dst = &x
		}
	}
	diff := func(dst **bool, v, def bool) {
		if v != def {
			*dst = &v
		}
	}
	dur(&j.LockDuration, p.LockDuration, d.LockDuration)
	if p.MaxSizeInMegabytes != d.MaxSizeInMegabytes {
		j.MaxSizeInMegabytes = &p.MaxSizeInMegabytes
	}
	diff(&j.RequiresDuplicateDetection, p.RequiresDuplicateDetection, d.RequiresDuplicateDetection)
	diff(&j.RequiresSession, p.RequiresSession, d.RequiresSession)
	dur(&j.DefaultMessageTimeToLive, p.DefaultMessageTimeToLive, d.DefaultMessageTimeToLive)
	diff(&j.DeadLetteringOnMessageExpiration, p.DeadLetteringOnMessageExpiration, d.DeadLetteringOnMessageExpiration)
	diff(&j.DeadLetteringOnFilterEvaluationExceptions, p.DeadLetteringOnFilterEvaluationExceptions, d.DeadLetteringOnFilterEvaluationExceptions)
	dur(&j.DuplicateDetectionHistoryTimeWindow, p.DuplicateDetectionHistoryTimeWindow, d.DuplicateDetectionHistoryTimeWindow)
	if p.MaxDeliveryCount != d.MaxDeliveryCount {
		j.MaxDeliveryCount = &p.MaxDeliveryCount
	}
	diff(&j.EnableBatchedOperations, p.EnableBatchedOperations, d.EnableBatchedOperations)
	if p.Status != d.Status {
		j.Status = &p.Status
	}
	dur(&j.AutoDeleteOnIdle, p.AutoDeleteOnIdle, d.AutoDeleteOnIdle)
	diff(&j.EnablePartitioning, p.EnablePartitioning, d.EnablePartitioning)
	diff(&j.EnableExpress, p.EnableExpress, d.EnableExpress)
	diff(&j.SupportOrdering, p.SupportOrdering, d.SupportOrdering)
	if p.ForwardTo != "" {
		j.ForwardTo = &p.ForwardTo
	}
	if p.ForwardDeadLetteredMessagesTo != "" {
		j.ForwardDeadLetteredMessagesTo = &p.ForwardDeadLetteredMessagesTo
	}
	if p.MaxMessageSizeInKilobytes != d.MaxMessageSizeInKilobytes {
		j.MaxMessageSizeInKilobytes = &p.MaxMessageSizeInKilobytes
	}
	if p.UserMetadata != "" {
		j.UserMetadata = &p.UserMetadata
	}
	return j
}
