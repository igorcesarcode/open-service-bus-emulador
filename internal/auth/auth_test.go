package auth

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

type clock struct{ t time.Time }

func (c *clock) Now() time.Time { return c.t }

func newAuthority(t *testing.T, mode Mode, local bool) (*Authority, *clock) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, store.Memory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	c := &clock{time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	a, err := New(ctx, st, Options{Mode: mode, DisableLocalAuth: !local, Now: c.Now})
	if err != nil {
		t.Fatal(err)
	}
	return a, c
}

func TestRights(t *testing.T) {
	if !Manage.Has(Listen) || !Manage.Has(Send) {
		t.Error("Manage does not imply Listen and Send")
	}
	if Send.Has(Listen) {
		t.Error("Send implies Listen")
	}
	r, err := ParseRights([]string{"listen", "SEND"})
	if err != nil || r != Listen|Send {
		t.Errorf("ParseRights = %v, %v", r, err)
	}
	if _, err := ParseRights([]string{"read"}); err == nil {
		t.Error("an unknown right was accepted")
	}
	if _, err := ParseRights(nil); err == nil {
		t.Error("no rights was accepted")
	}
}

func TestModeNoneHasNoRootPolicy(t *testing.T) {
	a, _ := newAuthority(t, ModeNone, true)
	if a.Enabled() || len(a.Policies("", true)) != 0 {
		t.Errorf("none mode: enabled = %v, policies = %v", a.Enabled(), a.Policies("", true))
	}
	if m, err := ParseMode(""); err != nil || m != ModeNone {
		t.Errorf("the default mode = %v, %v; want none", m, err)
	}
	if _, err := ParseMode("oauth"); err == nil {
		t.Error("an unknown mode was accepted")
	}
}

func TestSAS(t *testing.T) {
	ctx := context.Background()
	a, c := newAuthority(t, ModeSAS, true)
	if p, ok := a.Policy("", RootPolicyName); !ok || p.PrimaryKey != RootKey || !p.Rights.Has(Manage) {
		t.Fatalf("root policy = %+v, %v", p, ok)
	}
	if err := a.SavePolicy(ctx, Policy{Scope: "Orders", Name: "sender", Rights: Send, PrimaryKey: "k1", SecondaryKey: "k2"}); err != nil {
		t.Fatal(err)
	}
	exp := c.t.Add(time.Hour)
	auth := func(token, audience string) (*Claims, error) {
		return a.Authenticate(ctx, "servicebus.windows.net:sastoken", audience, token)
	}

	// The development key from the connection string.
	cl, err := auth(SASToken("sb://host/orders", RootPolicyName, RootKey, exp), "sb://host/orders")
	if err != nil || !cl.Allows("orders", Manage, c.t) || cl.Principal != RootPolicyName {
		t.Fatalf("root token = %+v, %v", cl, err)
	}
	// A token for one entity gives nothing elsewhere, even with the root key.
	if cl.Allows("payments", Send, c.t) {
		t.Error("a token for orders allows payments")
	}
	// A namespace token covers every entity.
	cl, err = auth(SASToken("sb://host", RootPolicyName, RootKey, exp), "sb://host")
	if err != nil || !cl.Allows("payments", Listen, c.t) || !cl.Allows("t/subscriptions/s", Send, c.t) {
		t.Errorf("namespace token = %+v, %v", cl, err)
	}

	// An entity policy: its rights, only its entity, either key.
	for _, key := range []string{"k1", "k2"} {
		cl, err = auth(SASToken("sb://host/orders", "sender", key, exp), "sb://host/orders")
		if err != nil {
			t.Fatalf("key %s: %v", key, err)
		}
		if !cl.Allows("orders", Send, c.t) || cl.Allows("orders", Listen, c.t) {
			t.Errorf("key %s: send-only policy claims = %+v", key, cl.Grants)
		}
	}
	if _, err = auth(SASToken("sb://host/payments", "sender", "k1", exp), "sb://host/payments"); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("entity policy used on another entity: %v", err)
	}

	// Bad signature, unknown policy and malformed tokens.
	for name, tok := range map[string]string{
		"wrong key":      SASToken("sb://host/orders", "sender", "nope", exp),
		"unknown policy": SASToken("sb://host/orders", "ghost", "k1", exp),
		"missing parts":  "SharedAccessSignature sr=x",
		"garbage":        "hello",
	} {
		if _, err := auth(tok, "sb://host/orders"); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("%s: error = %v, want ErrInvalidToken", name, err)
		}
	}
	// A token signed for another resource does not verify for this one.
	other := SASToken("sb://host/payments", "sender", "k1", exp)
	other = replaceResource(other, "sb://host/payments", "sb://host/orders")
	if _, err := auth(other, "sb://host/orders"); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("a token moved to another resource: %v", err)
	}

	// Expiry, at the claims and at the token.
	tok := SASToken("sb://host/orders", RootPolicyName, RootKey, c.t.Add(time.Minute))
	cl, err = auth(tok, "sb://host/orders")
	if err != nil {
		t.Fatal(err)
	}
	c.t = c.t.Add(2 * time.Minute)
	if cl.Allows("orders", Send, c.t) {
		t.Error("expired claims still allow")
	}
	if _, err := auth(tok, "sb://host/orders"); !errors.Is(err, ErrExpiredToken) {
		t.Errorf("expired token: %v", err)
	}
}

func replaceResource(token, from, to string) string {
	// Both resources are URL-escaped in the token.
	return strings.ReplaceAll(token, url.QueryEscape(from), url.QueryEscape(to))
}

func TestRegenerateKeyInvalidatesTokens(t *testing.T) {
	ctx := context.Background()
	a, c := newAuthority(t, ModeSAS, true)
	exp := c.t.Add(time.Hour)
	tok := SASToken("sb://host", RootPolicyName, RootKey, exp)
	if _, err := a.Authenticate(ctx, "", "sb://host", tok); err != nil {
		t.Fatal(err)
	}
	p, err := a.RegenerateKey(ctx, "", RootPolicyName, false)
	if err != nil || p.PrimaryKey == RootKey {
		t.Fatalf("RegenerateKey() = %+v, %v", p, err)
	}
	if _, err := a.Authenticate(ctx, "", "sb://host", tok); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("a token of the old key: %v", err)
	}
	if _, err := a.Authenticate(ctx, "", "sb://host", SASToken("sb://host", RootPolicyName, p.PrimaryKey, exp)); err != nil {
		t.Errorf("a token of the new key: %v", err)
	}
	// The secondary key kept working throughout.
	sec, _ := a.Policy("", RootPolicyName)
	if _, err := a.Authenticate(ctx, "", "sb://host", SASToken("sb://host", RootPolicyName, sec.SecondaryKey, exp)); err != nil {
		t.Errorf("the secondary key: %v", err)
	}
	if err := a.DeletePolicy(ctx, "", RootPolicyName); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Authenticate(ctx, "", "sb://host", SASToken("sb://host", RootPolicyName, p.PrimaryKey, exp)); err == nil {
		t.Error("a token of a deleted policy was accepted")
	}
}

func TestLocalAuthDisabled(t *testing.T) {
	a, c := newAuthority(t, ModeSAS, false)
	_, err := a.Authenticate(context.Background(), "", "sb://host", SASToken("sb://host", RootPolicyName, RootKey, c.t.Add(time.Hour)))
	if !errors.Is(err, ErrLocalAuthOff) {
		t.Errorf("SAS with local auth disabled: %v", err)
	}
}

func TestPoliciesSurviveReload(t *testing.T) {
	ctx := context.Background()
	st, _ := store.Open(ctx, store.Memory)
	defer st.Close()
	a, _ := New(ctx, st, Options{Mode: ModeSAS})
	a.SavePolicy(ctx, Policy{Scope: "q", Name: "p", Rights: Listen, PrimaryKey: "x", SecondaryKey: "y"})
	a.AddAssignment(ctx, "oid-1", RoleSender, "q")
	b, err := New(ctx, st, Options{Mode: ModeSAS})
	if err != nil {
		t.Fatal(err)
	}
	if p, ok := b.Policy("q", "p"); !ok || p.PrimaryKey != "x" || p.Rights != Listen {
		t.Errorf("reloaded policy = %+v, %v", p, ok)
	}
	if as := b.Assignments(); len(as) != 1 || as[0].Principal != "oid-1" || as[0].Role != RoleSender {
		t.Errorf("reloaded assignments = %+v", as)
	}
	// A second add of the same assignment is a no-op.
	b.AddAssignment(ctx, "oid-1", RoleSender, "q")
	if len(b.Assignments()) != 1 {
		t.Error("a duplicate assignment was added")
	}
}

func TestEntityFromResource(t *testing.T) {
	for in, want := range map[string]string{
		"sb://host/Orders":                   "orders",
		"amqp://host:5672/t/Subscriptions/S": "t/subscriptions/s",
		"sb://host/q/$management":            "q",
		"sb://host":                          "",
		"http://127.0.0.1:5672/":             "",
		"orders":                             "orders",
	} {
		if got := EntityFromResource(in); got != want {
			t.Errorf("EntityFromResource(%q) = %q, want %q", in, got, want)
		}
	}
}

func newFull(t *testing.T) (*Authority, *DevIssuer, *clock) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, store.Memory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	dev, err := NewDevIssuer("http://dev")
	if err != nil {
		t.Fatal(err)
	}
	c := &clock{time.Now()}
	dev.now = c.Now
	a, err := New(ctx, st, Options{Mode: ModeFull, Entra: dev.Entra(), Now: c.Now})
	if err != nil {
		t.Fatal(err)
	}
	return a, dev, c
}

func TestJWTRoles(t *testing.T) {
	ctx := context.Background()
	a, dev, c := newFull(t)
	a.AddAssignment(ctx, "owner-oid", RoleOwner, "")
	a.AddAssignment(ctx, "sender-oid", RoleSender, "orders")
	a.AddAssignment(ctx, "receiver-oid", RoleReceiver, "")

	claimsOf := func(oid string) *Claims {
		t.Helper()
		tok, _, err := dev.Token(oid, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		cl, err := a.Authenticate(ctx, "jwt", "sb://host", tok)
		if err != nil {
			t.Fatalf("Authenticate(%s): %v", oid, err)
		}
		return cl
	}
	owner := claimsOf("owner-oid")
	if !owner.Allows("anything", Manage, c.t) || owner.Source != "jwt" || owner.Principal != "owner-oid" {
		t.Errorf("owner claims = %+v", owner)
	}
	sender := claimsOf("sender-oid")
	if !sender.Allows("orders", Send, c.t) || sender.Allows("orders", Listen, c.t) || sender.Allows("payments", Send, c.t) {
		t.Errorf("sender claims = %+v", sender.Grants)
	}
	receiver := claimsOf("receiver-oid")
	if !receiver.Allows("payments", Listen, c.t) || receiver.Allows("payments", Send, c.t) || receiver.Allows("payments", Manage, c.t) {
		t.Errorf("receiver claims = %+v", receiver.Grants)
	}
	// A valid token of a principal with no role can do nothing.
	if nobody := claimsOf("nobody"); nobody.AllowsAny("orders", c.t) {
		t.Errorf("a principal without roles has claims: %+v", nobody.Grants)
	}
	// The Bearer prefix of an HTTP header is accepted.
	tok, _, _ := dev.Token("owner-oid", time.Hour)
	if _, err := a.Authenticate(ctx, "", "", "Bearer "+tok); err != nil {
		t.Errorf("Bearer token: %v", err)
	}
}

func TestJWTRejections(t *testing.T) {
	ctx := context.Background()
	a, dev, c := newFull(t)
	a.AddAssignment(ctx, "oid", RoleOwner, "")
	good, _, _ := dev.Token("oid", time.Hour)

	other, _ := NewDevIssuer("http://dev")
	forged, _, _ := other.Token("oid", time.Hour)
	parts := strings.Split(good, ".")
	tamperedClaims := parts[0] + "." + strings.Replace(parts[1], parts[1][:4], "AAAA", 1) + "." + parts[2]
	wrongIssuer, _ := NewDevIssuer("http://elsewhere")
	wrongIssuer.kid = dev.kid
	wrongIssuer.key = dev.key
	badIss, _, _ := wrongIssuer.Token("oid", time.Hour)

	for name, tok := range map[string]string{
		"signed by another key": forged,
		"tampered claims":       tamperedClaims,
		"not a jwt":             "abc",
		"wrong issuer":          badIss,
	} {
		if _, err := a.Authenticate(ctx, "jwt", "", tok); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("%s: error = %v, want ErrInvalidToken", name, err)
		}
	}
	// Expiry.
	short, _, _ := dev.Token("oid", 2*time.Minute)
	c.t = c.t.Add(10 * time.Minute)
	if _, err := a.Authenticate(ctx, "jwt", "", short); !errors.Is(err, ErrExpiredToken) {
		t.Errorf("expired token: %v", err)
	}
	// SAS-only mode refuses JWTs.
	sas, _ := newAuthority(t, ModeSAS, true)
	if _, err := sas.Authenticate(ctx, "jwt", "", good); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("JWT in sas mode: %v", err)
	}
	// Local auth off still leaves JWTs working.
	a.opts.DisableLocalAuth = true
	c.t = time.Now()
	fresh, _, _ := dev.Token("oid", time.Hour)
	if _, err := a.Authenticate(ctx, "jwt", "", fresh); err != nil {
		t.Errorf("JWT with local auth disabled: %v", err)
	}
}

func TestParseJWKSRoundTrip(t *testing.T) {
	dev, _ := NewDevIssuer("i")
	keys, err := parseJWKS(dev.JWKS())
	if err != nil || keys[dev.kid] == nil || keys[dev.kid].N.Cmp(dev.key.N) != 0 || keys[dev.kid].E != dev.key.E {
		t.Fatalf("parseJWKS = %v, %v", keys, err)
	}
	if _, err := parseJWKS([]byte(`{"keys": []}`)); err == nil {
		t.Error("an empty JWKS was accepted")
	}
}

func TestRoleParsing(t *testing.T) {
	for in, want := range map[string]Role{"owner": RoleOwner, "Azure Service Bus Data Sender": RoleSender, "RECEIVER": RoleReceiver} {
		if got, err := ParseRole(in); err != nil || got != want {
			t.Errorf("ParseRole(%q) = %v, %v", in, got, err)
		}
	}
	if _, err := ParseRole("contributor"); err == nil {
		t.Error("an unknown role was accepted")
	}
	a, _ := newAuthority(t, ModeFull, true)
	if err := a.AddAssignment(context.Background(), "x", Role("Reader"), ""); err == nil {
		t.Error("an assignment with a non-Service Bus role was accepted")
	}
}
