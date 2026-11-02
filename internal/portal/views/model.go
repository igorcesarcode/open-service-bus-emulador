// Package views holds the HTML components of the portal and the view models
// they render.
package views

// Info describes the emulator in the Essentials block of the overview.
type Info struct {
	Name            string
	Status          string
	Version         string
	Database        string
	Uptime          string
	Endpoint        string
	ConnectionStr   string // shared port
	AMQPConnection  string
	HTTPConnection  string
	HasCertificate  bool
	Queues, Topics  int
	ActivityEnabled bool
}

// Nav identifies the page highlighted in the side menu.
type Nav string

// Pages of the side menu.
const (
	NavOverview    Nav = "overview"
	NavActivity    Nav = "activity"
	NavMetrics     Nav = "metrics"
	NavPolicies    Nav = "policies"
	NavAccess      Nav = "access"
	NavGeoDR       Nav = "geodr"
	NavGeoRep      Nav = "georep"
	NavProps       Nav = "props"
	NavExport      Nav = "export"
	NavUnsupported Nav = "unsupported"
	NavQueues      Nav = "queues"
	NavTopics      Nav = "topics"
)

// Layout is what every page shares.
type Layout struct {
	Title string
	Name  string
	Nav   Nav
	// SignOut shows the sign out button, when the portal needs a login.
	SignOut bool
	// Namespaces lists the namespaces of the server for the switcher in the
	// header, with the current one selected. Empty with a single namespace.
	Namespaces []Option
}

// RangeOption is an interval of the "Show data for the last" selector.
type RangeOption struct {
	Key, Label string
	Active     bool
}

// Tick is a labelled position on a chart axis.
type Tick struct {
	Pos   float64
	Label string
}

// Series is one line of a chart.
type Series struct {
	Name  string
	Class string // CSS class selecting the color
	Total string
	Path  string // SVG path data
}

// Chart is a line chart drawn as SVG on a fixed viewBox.
type Chart struct {
	Title         string
	Width, Height int
	// Left, Right, Top and Bottom bound the plot area.
	Left, Right, Top, Bottom int
	Series                   []Series
	XTicks                   []Tick
	YTicks                   []Tick
}

// Overview is the data of the namespace overview.
type Overview struct {
	Info     Info
	Ranges   []RangeOption
	Range    string
	Requests Chart
	Messages Chart
}

// EntityRow is a row of the queues or topics list.
type EntityRow struct {
	Name          string
	Path          string
	Status        string
	Active        int64
	DeadLetter    int64
	Scheduled     int64
	Size          string
	MaxSize       string
	MaxDelivery   int32
	Subscriptions int
}

// EntityList is the data of the queues or topics page.
type EntityList struct {
	Kind  string // "queue" or "topic"
	Query string
	Rows  []EntityRow
}

// Field is a form field with its current value.
type Field struct {
	Name, Label, Value, Help string
}

// EntityForm is the form that creates or edits a queue, topic or
// subscription.
type EntityForm struct {
	Kind     string // "queue", "topic" or "subscription"
	Edit     bool
	Error    string
	Name     string
	Topic    string // path of the topic of a subscription
	Path     string // path of the entity being edited
	Action   string
	Cancel   string
	Text     []Field
	Checks   []Check
	Status   string
	Statuses []string
}

// Check is a boolean form field.
type Check struct {
	Name, Label string
	On          bool
	Disabled    bool
}

// Tile is a headline number of an entity page.
type Tile struct {
	Label, Value, Unit string
	Link               string
}

// KV is a labelled value.
type KV struct{ Key, Value string }

// SubscriptionRow is a row of the subscriptions of a topic.
type SubscriptionRow struct {
	Name        string
	Path        string
	Status      string
	Messages    int64
	MaxDelivery int32
}

// EntityPage is the overview of a queue, topic or subscription.
type EntityPage struct {
	Kind, Name, Path string
	TopicPath        string // of a subscription
	TopicName        string
	Props            []KV
	Live             EntityLive
	Subscriptions    []SubscriptionRow
	Query            string
}

// EntityLive is the part of an entity page that refreshes.
type EntityLive struct {
	Path     string
	Tiles    []Tile
	Counts   []Tile
	Ranges   []RangeOption
	Range    string
	Requests Chart
	Messages Chart
}

// RuleRow is a row of the rules of a subscription.
type RuleRow struct {
	Name, Type, Filter, Action string
}

// RulesPage is the rules page of a subscription.
type RulesPage struct {
	Path, Name, TopicName string
	Rules                 []RuleRow
	Sample                string
	Results               []RuleResultView
	Tested                bool
}

// RuleResultView is the outcome of one rule on the sample message.
type RuleResultView struct {
	Name    string
	Matched bool
	Error   string
	Changes []string
}

// RuleForm is the form that creates or edits a rule.
type RuleForm struct {
	Path, SubName string
	Edit          bool
	Error         string
	Name          string
	Kind          string
	Kinds         []string
	SQL           string
	Action        string
	Parameters    string
	Correlation   []Field
	CorrProps     string
}

// ActivityRow is a row of the activity log.
type ActivityRow struct {
	Time      string
	Actor     string
	Operation string
	Entity    string
	Status    string
	Detail    string
	Failed    bool
}

// ActivityPage is the data of the activity log page.
type ActivityPage struct {
	Actor, Operation, Entity, Status string
	Rows                             []ActivityRow
}

// Prop is an application property with its type.
type Prop struct{ Key, Type, Value string }

// MessageRow is a row of the Explorer table.
type MessageRow struct {
	Seq           int64
	MessageID     string
	Enqueued      string
	DeliveryCount int
	State         string
	BodySize      string
	Subject       string
	Text          string
	SessionID     string
	// LockToken and LockedUntil are set in Receive Mode.
	LockToken   string
	LockedUntil string
	// Held is the opaque value that carries a received message between
	// requests: sequence number, lock token and lock expiry.
	Held string
}

// MessageDetail is the detail panel of a message.
type MessageDetail struct {
	Seq      int64
	Body     string
	BodyKind string // "json", "text", "hex" or "empty"
	Broker   []KV
	Custom   []Prop
}

// ExplorerTab is a tab of the Explorer: the active queue or a dead-letter
// queue.
type ExplorerTab struct {
	Key, Label string
	Count      int64
	Active     bool
}

// Explorer is the state of the Service Bus Explorer for one entity.
type Explorer struct {
	Path, Name string
	Kind       string
	TopicPath  string
	TopicName  string
	Mode       string // "peek" or "receive"
	Queue      string // "active", "dlq" or "tdlq"
	Tabs       []ExplorerTab
	Rows       []MessageRow
	Shown      int
	Total      int64
	Next       int64 // sequence number to peek next from
	From       int64
	Count      int
	UTC        bool
	Message    string
	Error      string
	// Subscriptions lists what to pick on a topic, which holds no messages.
	Subscriptions []SubscriptionRow
	// Selected is the sequence number open in the detail panel.
	Selected int64
	// CanDeadLetter is false in a dead-letter queue.
	CanDeadLetter bool
}

// SendPanel is the Send messages side panel.
type SendPanel struct {
	Path, Name   string
	Queue        string
	ContentTypes []string
	ContentType  string
	Body         string
	Fields       []Field
	Custom       []Prop
	Types        []string
	Error        string
	Repeat       int
	IntervalMs   int
}

// Option is a choice of a select list.
type Option struct {
	Value, Label string
	Selected     bool
}

// MetricOption is a checkbox of the Metrics page.
type MetricOption struct {
	Name, Label string
	On          bool
}

// MetricsPage is the free-form Metrics page.
type MetricsPage struct {
	Entities []Option
	Counters []MetricOption
	Gauges   []MetricOption
	Ranges   []RangeOption
	Range    string
	APIURL   string
	Charts   []Chart
}

// PolicyRow is a row of the shared access policies table.
type PolicyRow struct {
	Scope, ScopeLabel, Name, Rights string
	PrimaryKey, SecondaryKey        string
	Connection                      string
}

// PoliciesPage is the shared access policies page.
type PoliciesPage struct {
	AuthMode string
	Rows     []PolicyRow
	Scopes   []Option
	Error    string
}

// AssignmentRow is a role assignment of the access control page.
type AssignmentRow struct {
	ID              int64
	Principal, Role string
	ScopeLabel      string
}

// AccessPage is the access control (IAM) page.
type AccessPage struct {
	AuthMode string
	Rows     []AssignmentRow
	Scopes   []Option
	Roles    []string
	Issuer   bool
	Error    string
}

// QueueLabel names the queue the Explorer shows, for confirmations.
func (e Explorer) QueueLabel() string {
	switch e.Queue {
	case "dlq":
		return "dead-letter queue"
	case "tdlq":
		return "transfer dead-letter queue"
	}
	return "active queue"
}

// GeoDRRow is an alias of the Geo-Recovery page.
type GeoDRRow struct {
	Alias, Primary, Secondary string
	Paired                    bool
	LastSync, Error           string
	// Role is what the page's namespace is for the alias: primary,
	// secondary or none.
	Role string
}

// GeoDRPage is the Geo-Recovery page of a namespace.
type GeoDRPage struct {
	Namespace  string
	Rows       []GeoDRRow
	Namespaces []Option
	Message    string
	Error      string
}

// GeoReplicationView is the state of the Geo-Replication page.
type GeoReplicationView struct {
	Namespace                      string
	Enabled                        bool
	Mode                           string
	MaxLag, Delay                  string
	PrimaryRegion, SecondaryRegion string
	State                          string
	LagDuration                    string
	LagCount                       int
	Promoting                      bool
	LastPromotion                  string
	LostChanges                    int
	Message, Error                 string
}

// LockRow is a resource lock of the properties page.
type LockRow struct {
	Name, Level, Scope, Notes string
}

// PropertiesPage is the namespace's tags, locks and encryption.
type PropertiesPage struct {
	Namespace string
	Message   string
	Error     string
	// Tags is one name=value per line.
	Tags  string
	Locks []LockRow
	// Scopes lists the namespace and its entities for a new lock.
	Scopes []Option
	// Encryption fields; Encrypted is set when it is configured.
	Encrypted      bool
	KeyIDs         string
	Identity       string
	Infrastructure bool
}

// ExportFile is a file of the export template page.
type ExportFile struct {
	Name    string
	Size    int
	Preview string
}

// ExportPage offers the namespaces' state as files to download.
type ExportPage struct {
	Files []ExportFile
	Error string
}
