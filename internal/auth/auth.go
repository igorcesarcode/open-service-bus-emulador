// Package auth decides who may do what in a namespace: shared access
// signature (SAS) tokens checked against shared access policies, and, in
// full mode, Entra ID tokens checked against role assignments.
//
// Authentication is off by default (ModeNone): every client is accepted,
// as in the official emulator. ModeSAS and ModeFull turn on the checks,
// which the AMQP and management layers apply to every connection and
// request.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

// Mode says which checks are on.
type Mode string

// Modes.
const (
	// ModeNone accepts every client.
	ModeNone Mode = "none"
	// ModeSAS requires a valid SAS token.
	ModeSAS Mode = "sas"
	// ModeFull also accepts Entra ID tokens, authorized by role assignments.
	ModeFull Mode = "full"
)

// ParseMode reads the value of -auth.
func ParseMode(s string) (Mode, error) {
	switch m := Mode(strings.ToLower(s)); m {
	case "", ModeNone:
		return ModeNone, nil
	case ModeSAS, ModeFull:
		return m, nil
	}
	return "", fmt.Errorf("unknown auth mode %q: want none, sas or full", s)
}

// Right is a permission of a shared access policy.
type Right uint8

// Rights. Manage includes Listen and Send, as in Service Bus.
const (
	Listen Right = 1 << iota
	Send
	Manage
)

// Normalize makes Manage imply the other rights.
func (r Right) Normalize() Right {
	if r&Manage != 0 {
		return Listen | Send | Manage
	}
	return r
}

// Has reports whether r includes every right of x.
func (r Right) Has(x Right) bool { return r.Normalize()&x == x }

func (r Right) String() string { return strings.Join(r.Names(), ",") }

// Names lists the rights as Service Bus names them.
func (r Right) Names() []string {
	var out []string
	for _, n := range []struct {
		r    Right
		name string
	}{{Listen, "Listen"}, {Send, "Send"}, {Manage, "Manage"}} {
		if r&n.r != 0 {
			out = append(out, n.name)
		}
	}
	return out
}

// ParseRights reads right names, case-insensitively.
func ParseRights(names []string) (Right, error) {
	var r Right
	for _, n := range names {
		switch strings.ToLower(strings.TrimSpace(n)) {
		case "listen":
			r |= Listen
		case "send":
			r |= Send
		case "manage":
			r |= Manage
		default:
			return 0, fmt.Errorf("unknown right %q: want Listen, Send or Manage", n)
		}
	}
	if r == 0 {
		return 0, errors.New("a policy needs at least one right")
	}
	return r, nil
}

// Policy is a shared access policy. Scope is the normalized path of a queue
// or topic, or "" for the namespace.
type Policy struct {
	Scope        string
	Name         string
	Rights       Right
	PrimaryKey   string
	SecondaryKey string
}

// RootPolicyName and RootKey are the namespace policy the development
// connection string of the SDKs uses.
const (
	RootPolicyName = "RootManageSharedAccessKey"
	RootKey        = "SAS_KEY_VALUE"
)

// Options configures an Authority.
type Options struct {
	Mode Mode
	// DisableLocalAuth rejects SAS tokens, as local_auth_enabled = false
	// does in Azure. Only Entra ID tokens work then.
	DisableLocalAuth bool
	// Now overrides the clock, for tests.
	Now func() time.Time
	// Entra configures token validation in ModeFull.
	Entra *Entra
}

// Authority holds the policies and role assignments of a namespace.
type Authority struct {
	opts  Options
	store *store.Ref
	now   func() time.Time

	hookMu   sync.Mutex
	onChange func()

	mu          sync.RWMutex
	policies    map[policyKey]*Policy
	assignments []Assignment
}

type policyKey struct{ scope, name string }

// New loads the policies and assignments in st. In a mode that checks
// tokens it makes sure the namespace has RootManageSharedAccessKey.
func New(ctx context.Context, st *store.Store, opts Options) (*Authority, error) {
	if opts.Mode == "" {
		opts.Mode = ModeNone
	}
	a := &Authority{opts: opts, store: store.NewRef(st), now: opts.Now, policies: map[policyKey]*Policy{}}
	if a.now == nil {
		a.now = time.Now
	}
	if err := a.reload(ctx); err != nil {
		return nil, err
	}
	if a.Enabled() && a.opts.Mode != ModeNone {
		if _, ok := a.policies[policyKey{"", RootPolicyName}]; !ok {
			err := a.SavePolicy(ctx, Policy{Name: RootPolicyName, Rights: Manage, PrimaryKey: RootKey, SecondaryKey: newKey()})
			if err != nil {
				return nil, err
			}
		}
	}
	return a, nil
}

// reload reads the policies and assignments of the current store.
func (a *Authority) reload(ctx context.Context) error {
	policies := map[policyKey]*Policy{}
	var assignments []Assignment
	err := a.store.Read(ctx, func(tx *store.Tx) error {
		rows, err := tx.Policies()
		if err != nil {
			return err
		}
		for _, r := range rows {
			policies[policyKey{r.Scope, r.Name}] = &Policy{Scope: r.Scope, Name: r.Name, Rights: Right(r.Rights), PrimaryKey: r.PrimaryKey, SecondaryKey: r.SecondaryKey}
		}
		as, err := tx.Assignments()
		for _, x := range as {
			assignments = append(assignments, Assignment{ID: x.ID, Principal: x.Principal, Role: Role(x.Role), Scope: x.Scope})
		}
		return err
	})
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.policies, a.assignments = policies, assignments
	a.mu.Unlock()
	return nil
}

// SwapStore makes st, a copy of the current store, the authority's store,
// and reloads the policies and role assignments from it.
func (a *Authority) SwapStore(ctx context.Context, st *store.Store) error {
	a.store.Set(st)
	return a.reload(ctx)
}

// Mode returns the mode the authority runs in.
func (a *Authority) Mode() Mode { return a.opts.Mode }

// Enabled reports whether clients must authenticate.
func (a *Authority) Enabled() bool { return a != nil && a.opts.Mode != ModeNone }

// LocalAuthEnabled reports whether SAS tokens are accepted.
func (a *Authority) LocalAuthEnabled() bool { return !a.opts.DisableLocalAuth }

func newKey() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

// NewKey returns a random policy key.
func NewKey() string { return newKey() }

func normScope(s string) string { return strings.ToLower(strings.Trim(s, "/")) }

// OnChange sets a function called, without blocking, after a policy is
// saved or deleted. A later call replaces it.
func (a *Authority) OnChange(fn func()) {
	a.hookMu.Lock()
	a.onChange = fn
	a.hookMu.Unlock()
}

func (a *Authority) changed() {
	a.hookMu.Lock()
	fn := a.onChange
	a.hookMu.Unlock()
	if fn != nil {
		fn()
	}
}

// SavePolicy creates a policy or replaces the one with its scope and name.
// Empty keys are generated.
func (a *Authority) SavePolicy(ctx context.Context, p Policy) error {
	p.Scope = normScope(p.Scope)
	if p.Name == "" {
		return errors.New("a policy needs a name")
	}
	if p.Rights == 0 {
		return errors.New("a policy needs at least one right")
	}
	if p.PrimaryKey == "" {
		p.PrimaryKey = newKey()
	}
	if p.SecondaryKey == "" {
		p.SecondaryKey = newKey()
	}
	err := a.store.Write(ctx, func(tx *store.Tx) error {
		return tx.SavePolicy(store.Policy{Scope: p.Scope, Name: p.Name, Rights: int(p.Rights), PrimaryKey: p.PrimaryKey, SecondaryKey: p.SecondaryKey})
	})
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.policies[policyKey{p.Scope, p.Name}] = &p
	a.mu.Unlock()
	a.changed()
	return nil
}

// Declare applies a declared policy: it creates the policy, or updates the
// rights of an existing one. Keys left empty in the declaration keep the
// existing ones, so restarting does not change them.
func (a *Authority) Declare(ctx context.Context, p Policy) error {
	if old, ok := a.Policy(p.Scope, p.Name); ok {
		if p.PrimaryKey == "" {
			p.PrimaryKey = old.PrimaryKey
		}
		if p.SecondaryKey == "" {
			p.SecondaryKey = old.SecondaryKey
		}
		if old == (Policy{Scope: normScope(p.Scope), Name: p.Name, Rights: p.Rights, PrimaryKey: p.PrimaryKey, SecondaryKey: p.SecondaryKey}) {
			return nil
		}
	}
	return a.SavePolicy(ctx, p)
}

// DeletePolicy removes a policy. Tokens signed with it stop working for new
// connections.
func (a *Authority) DeletePolicy(ctx context.Context, scope, name string) error {
	scope = normScope(scope)
	if err := a.store.Write(ctx, func(tx *store.Tx) error { return tx.DeletePolicy(scope, name) }); err != nil {
		return err
	}
	a.mu.Lock()
	delete(a.policies, policyKey{scope, name})
	a.mu.Unlock()
	a.changed()
	return nil
}

// DeleteScope removes every policy of the scope, for an entity that was
// deleted.
func (a *Authority) DeleteScope(ctx context.Context, scope string) {
	for _, p := range a.Policies(scope, false) {
		a.DeletePolicy(ctx, scope, p.Name)
	}
}

// RegenerateKey replaces the primary or secondary key of a policy with a
// new random one.
func (a *Authority) RegenerateKey(ctx context.Context, scope, name string, secondary bool) (Policy, error) {
	p, ok := a.Policy(scope, name)
	if !ok {
		return Policy{}, fmt.Errorf("no policy %q on %q", name, scope)
	}
	if secondary {
		p.SecondaryKey = newKey()
	} else {
		p.PrimaryKey = newKey()
	}
	return p, a.SavePolicy(ctx, p)
}

// Policy returns a copy of a policy.
func (a *Authority) Policy(scope, name string) (Policy, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	p, ok := a.policies[policyKey{normScope(scope), name}]
	if !ok {
		return Policy{}, false
	}
	return *p, true
}

// Policies lists the policies of scope, or of every scope when all is true,
// sorted by scope and name.
func (a *Authority) Policies(scope string, all bool) []Policy {
	a.mu.RLock()
	defer a.mu.RUnlock()
	var out []Policy
	for k, p := range a.policies {
		if all || k.scope == normScope(scope) {
			out = append(out, *p)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Scope != out[j].Scope {
			return out[i].Scope < out[j].Scope
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// Grant lets the holder of a token use Rights on the entity at Scope and
// the entities below it. An empty Scope is the whole namespace.
type Grant struct {
	Scope  string
	Rights Right
}

// Claims are what an authenticated token allows.
type Claims struct {
	Grants    []Grant
	Expires   time.Time
	Principal string
	// Source is "sas" or "jwt".
	Source string
}

// covers reports whether scope is path or one of its ancestors.
func covers(scope, path string) bool {
	return scope == "" || path == scope || strings.HasPrefix(path, scope+"/")
}

// Allows reports whether the claims give right on the entity at path at
// time now.
func (c *Claims) Allows(path string, right Right, now time.Time) bool {
	if c == nil || (!c.Expires.IsZero() && !now.Before(c.Expires)) {
		return false
	}
	path = normScope(path)
	for _, g := range c.Grants {
		if covers(g.Scope, path) && g.Rights.Has(right) {
			return true
		}
	}
	return false
}

// AllowsAny reports whether the claims give any right on path.
func (c *Claims) AllowsAny(path string, now time.Time) bool {
	return c.Allows(path, Listen, now) || c.Allows(path, Send, now) || c.Allows(path, Manage, now)
}

// Errors reported by Authenticate.
var (
	ErrInvalidToken = errors.New("the token is not valid")
	ErrExpiredToken = errors.New("the token has expired")
	ErrLocalAuthOff = errors.New("shared access signature authentication is disabled")
)

// Authenticate validates a token presented in a CBS put-token request
// (tokenType "servicebus.windows.net:sastoken" or "jwt") or in an
// Authorization header. audience is the resource the token is for.
func (a *Authority) Authenticate(ctx context.Context, tokenType, audience string, token any) (*Claims, error) {
	s, _ := token.(string)
	if b, ok := token.([]byte); ok {
		s = string(b)
	}
	switch {
	case strings.EqualFold(tokenType, "jwt") || strings.HasPrefix(s, "Bearer "):
		if a.opts.Mode != ModeFull || a.opts.Entra == nil {
			return nil, fmt.Errorf("%w: Entra ID tokens are not accepted; start with -auth full", ErrInvalidToken)
		}
		return a.authenticateJWT(ctx, strings.TrimPrefix(s, "Bearer "))
	}
	if !a.LocalAuthEnabled() {
		return nil, ErrLocalAuthOff
	}
	return a.authenticateSAS(s)
}

// EntityFromResource extracts the entity path from a token resource such as
// "sb://host/queue" or "amqp://host/topic/$management".
func EntityFromResource(resource string) string {
	path := resource
	if i := strings.Index(path, "://"); i >= 0 {
		path = path[i+3:]
		if j := strings.IndexByte(path, '/'); j >= 0 {
			path = path[j+1:]
		} else {
			path = ""
		}
	}
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	path = strings.TrimSuffix(strings.Trim(path, "/"), "/$management")
	path = strings.TrimSuffix(path, "$management")
	return normScope(strings.Trim(path, "/"))
}

// ancestors lists path and the scopes above it, deepest first, ending with
// the namespace "".
func ancestors(path string) []string {
	out := []string{path}
	if i := strings.Index(path, "/subscriptions/"); i >= 0 {
		path = path[:i]
		out = append(out, path)
	}
	if path != "" {
		out = append(out, "")
	}
	return out
}

// authenticateSAS checks a "SharedAccessSignature sr=...&sig=...&se=...&skn=..."
// token against the policy it names.
func (a *Authority) authenticateSAS(token string) (*Claims, error) {
	token = strings.TrimSpace(strings.TrimPrefix(token, "SharedAccessSignature"))
	q, err := url.ParseQuery(token)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed", ErrInvalidToken)
	}
	sr, sig, se, skn := q.Get("sr"), q.Get("sig"), q.Get("se"), q.Get("skn")
	if sr == "" || sig == "" || se == "" || skn == "" {
		return nil, fmt.Errorf("%w: sr, sig, se and skn are required", ErrInvalidToken)
	}
	exp, err := strconv.ParseInt(se, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: bad expiry", ErrInvalidToken)
	}
	expires := time.Unix(exp, 0)
	scope := EntityFromResource(sr)

	a.mu.RLock()
	var found *Policy
	for _, s := range ancestors(scope) {
		if p, ok := a.policies[policyKey{s, skn}]; ok {
			c := *p
			found = &c
			break
		}
	}
	a.mu.RUnlock()
	if found == nil || !signatureMatches(found, sr, se, sig) {
		return nil, fmt.Errorf("%w: invalid signature", ErrInvalidToken)
	}
	if !a.now().Before(expires) {
		return nil, ErrExpiredToken
	}
	// The effective scope is the narrower of the token's and the policy's.
	eff := scope
	if !covers(found.Scope, scope) {
		return nil, fmt.Errorf("%w: policy %s does not cover %s", ErrInvalidToken, skn, scope)
	}
	return &Claims{Grants: []Grant{{Scope: eff, Rights: found.Rights}}, Expires: expires, Principal: skn, Source: "sas"}, nil
}

// signatureMatches checks sig against the HMAC of the resource and expiry
// with either key of the policy. The resource is signed in the escaped
// form (in either case) or as given, depending on the SDK.
func signatureMatches(p *Policy, sr, se, sig string) bool {
	escaped := url.QueryEscape(sr)
	for _, resource := range []string{escaped, strings.ToLower(escaped), sr} {
		for _, key := range []string{p.PrimaryKey, p.SecondaryKey} {
			mac := hmac.New(sha256.New, []byte(key))
			mac.Write([]byte(resource + "\n" + se))
			want := base64.StdEncoding.EncodeToString(mac.Sum(nil))
			if subtle.ConstantTimeCompare([]byte(want), []byte(sig)) == 1 {
				return true
			}
		}
	}
	return false
}

// SASToken builds a token for resource signed with key, valid until
// expires. It is what the SDKs do, and what the tests and the portal use.
func SASToken(resource, keyName, key string, expires time.Time) string {
	se := strconv.FormatInt(expires.Unix(), 10)
	escaped := url.QueryEscape(resource)
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(escaped + "\n" + se))
	sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("SharedAccessSignature sr=%s&sig=%s&se=%s&skn=%s", escaped, url.QueryEscape(sig), se, keyName)
}

// ---- Roles ----

// Role is a Service Bus data role.
type Role string

// Roles.
const (
	RoleOwner    Role = "Azure Service Bus Data Owner"
	RoleSender   Role = "Azure Service Bus Data Sender"
	RoleReceiver Role = "Azure Service Bus Data Receiver"
)

// Rights returns what the role allows.
func (r Role) Rights() Right {
	switch r {
	case RoleOwner:
		return Manage
	case RoleSender:
		return Send
	case RoleReceiver:
		return Listen
	}
	return 0
}

// Assignment gives a principal (the object id of an Entra ID user or
// application) a role on a scope.
type Assignment struct {
	ID        int64
	Principal string
	Role      Role
	Scope     string
}

// ParseRole reads a role by its full name or by a short one (owner, sender,
// receiver).
func ParseRole(s string) (Role, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "owner", "data owner", strings.ToLower(string(RoleOwner)):
		return RoleOwner, nil
	case "sender", "data sender", strings.ToLower(string(RoleSender)):
		return RoleSender, nil
	case "receiver", "data receiver", strings.ToLower(string(RoleReceiver)):
		return RoleReceiver, nil
	}
	return "", fmt.Errorf("unknown role %q: want owner, sender or receiver", s)
}

// AddAssignment records a role assignment unless it exists.
func (a *Authority) AddAssignment(ctx context.Context, principal string, role Role, scope string) error {
	if principal == "" || role.Rights() == 0 {
		return errors.New("an assignment needs a principal and a Service Bus data role")
	}
	scope = normScope(scope)
	for _, x := range a.Assignments() {
		if x.Principal == principal && x.Role == role && x.Scope == scope {
			return nil
		}
	}
	err := a.store.Write(ctx, func(tx *store.Tx) error {
		return tx.AddAssignment(store.Assignment{Principal: principal, Role: string(role), Scope: scope})
	})
	if err != nil {
		return err
	}
	return a.reloadAssignments(ctx)
}

// DeleteAssignment removes an assignment by id.
func (a *Authority) DeleteAssignment(ctx context.Context, id int64) error {
	if err := a.store.Write(ctx, func(tx *store.Tx) error { return tx.DeleteAssignment(id) }); err != nil {
		return err
	}
	return a.reloadAssignments(ctx)
}

func (a *Authority) reloadAssignments(ctx context.Context) error {
	var rows []store.Assignment
	err := a.store.Read(ctx, func(tx *store.Tx) error {
		var err error
		rows, err = tx.Assignments()
		return err
	})
	if err != nil {
		return err
	}
	out := make([]Assignment, len(rows))
	for i, r := range rows {
		out[i] = Assignment{ID: r.ID, Principal: r.Principal, Role: Role(r.Role), Scope: r.Scope}
	}
	a.mu.Lock()
	a.assignments = out
	a.mu.Unlock()
	return nil
}

// Assignments lists the role assignments.
func (a *Authority) Assignments() []Assignment {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return append([]Assignment(nil), a.assignments...)
}

// grantsFor returns the grants of a principal from its role assignments.
func (a *Authority) grantsFor(principal string) []Grant {
	a.mu.RLock()
	defer a.mu.RUnlock()
	var out []Grant
	for _, x := range a.assignments {
		if x.Principal == principal {
			out = append(out, Grant{Scope: x.Scope, Rights: x.Role.Rights()})
		}
	}
	return out
}

// ErrUnauthorized reports a request without the claims it needs.
var ErrUnauthorized = errors.New("unauthorized")

// AuthorizeRequest checks the Authorization header of an HTTP request: its
// token must give right on scope, or on the namespace for scope "". It
// returns nil when authentication is off.
func (a *Authority) AuthorizeRequest(r *http.Request, scope string, right Right) error {
	if !a.Enabled() {
		return nil
	}
	token := r.Header.Get("Authorization")
	if token == "" {
		return fmt.Errorf("%w: missing Authorization header; use a SharedAccessSignature or Bearer token", ErrUnauthorized)
	}
	claims, err := a.Authenticate(r.Context(), "", r.URL.String(), token)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	if !claims.Allows(scope, right, a.now()) {
		return fmt.Errorf("%w: '%s' claim(s) are required to perform this operation. Resource: '%s'", ErrUnauthorized, strings.Join(right.Names(), "' and '"), scope)
	}
	return nil
}

// Login checks a policy name and key given by a person, as the portal's
// login form does: the policy must be a namespace policy with Manage.
func (a *Authority) Login(name, key string) error {
	p, ok := a.Policy("", name)
	if !ok || !p.Rights.Has(Manage) || !a.LocalAuthEnabled() {
		return errors.New("invalid policy or key")
	}
	if subtle.ConstantTimeCompare([]byte(key), []byte(p.PrimaryKey)) != 1 && subtle.ConstantTimeCompare([]byte(key), []byte(p.SecondaryKey)) != 1 {
		return errors.New("invalid policy or key")
	}
	return nil
}

// DeclaredKey returns the key of a policy declared in a file, which gives
// no key: derived from the scope and name, so a connection string written
// down once keeps working across restarts and machines. The derivation is
// not a secret; declared policies are for local use.
func DeclaredKey(scope, name string, secondary bool) string {
	which := "primary"
	if secondary {
		which = "secondary"
	}
	sum := sha256.Sum256([]byte("open-service-bus:" + normScope(scope) + ":" + name + ":" + which))
	return base64.StdEncoding.EncodeToString(sum[:])
}
