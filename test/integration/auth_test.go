package integration_test

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

// withKey rewrites the shared access policy of a connection string.
func withKey(connStr, name, key string) string {
	connStr = strings.Replace(connStr, "SharedAccessKeyName=RootManageSharedAccessKey", "SharedAccessKeyName="+name, 1)
	return strings.Replace(connStr, "SharedAccessKey=SAS_KEY_VALUE", "SharedAccessKey="+key, 1)
}

func authServer(t *testing.T, mode auth.Mode, edit func(*server.Options)) *server.Server {
	t.Helper()
	opts := server.Options{
		Auth:   mode,
		Queues: queues("orders", "payments"),
		Policies: []auth.Policy{
			{Name: "sender", Scope: "orders", Rights: auth.Send, PrimaryKey: "send-key"},
			{Name: "listener", Scope: "orders", Rights: auth.Listen, PrimaryKey: "listen-key"},
			{Name: "app", Rights: auth.Listen | auth.Send, PrimaryKey: "app-key"},
		},
	}
	if edit != nil {
		edit(&opts)
	}
	return startServer(t, opts)
}

// trySend sends one message and returns the error, giving up quickly.
func trySend(srv *server.Server, connStr, queue string) error {
	client, err := azservicebus.NewClientFromConnectionString(connStr, &azservicebus.ClientOptions{
		RetryOptions: azservicebus.RetryOptions{MaxRetries: -1},
	})
	if err != nil {
		return err
	}
	defer client.Close(context.Background())
	sender, err := client.NewSender(queue, nil)
	if err != nil {
		return err
	}
	defer sender.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return sender.SendMessage(ctx, &azservicebus.Message{Body: []byte("x")}, nil)
}

func tryReceive(connStr, queue string) error {
	client, err := azservicebus.NewClientFromConnectionString(connStr, &azservicebus.ClientOptions{
		RetryOptions: azservicebus.RetryOptions{MaxRetries: -1},
	})
	if err != nil {
		return err
	}
	defer client.Close(context.Background())
	r, err := client.NewReceiverForQueue(queue, nil)
	if err != nil {
		return err
	}
	defer r.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = r.PeekMessages(ctx, 1, nil)
	return err
}

func isUnauthorized(err error) bool {
	var sbErr *azservicebus.Error
	if errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeUnauthorizedAccess {
		return true
	}
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "unauthorized")
}

func TestAuthOffByDefault(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	// Whatever key the client signs with, nothing is checked.
	if err := trySend(srv, withKey(srv.ConnectionString(), "whatever", "nonsense"), "orders"); err != nil {
		t.Errorf("send with an arbitrary key and auth off: %v", err)
	}
	if srv.Authority().Enabled() || len(srv.Authority().Policies("", true)) != 0 {
		t.Error("auth is on by default")
	}
}

func TestSASPolicies(t *testing.T) {
	t.Parallel()
	srv := authServer(t, auth.ModeSAS, nil)
	root := srv.ConnectionString() // the development key
	sender := withKey(root, "sender", "send-key")
	listener := withKey(root, "listener", "listen-key")

	// The development connection string keeps working.
	if err := trySend(srv, root, "orders"); err != nil {
		t.Errorf("send with the root key: %v", err)
	}
	// A namespace policy with Listen and Send.
	app := withKey(root, "app", "app-key")
	if err := trySend(srv, app, "payments"); err != nil {
		t.Errorf("send with the app policy: %v", err)
	}
	// An entity policy works on its entity only, for the right it has.
	if err := trySend(srv, sender, "orders"); err != nil {
		t.Errorf("send with the Send policy: %v", err)
	}
	if err := tryReceive(sender, "orders"); !isUnauthorized(err) {
		t.Errorf("receive with a Send-only policy: %v, want unauthorized", err)
	}
	if err := tryReceive(listener, "orders"); err != nil {
		t.Errorf("receive with the Listen policy: %v", err)
	}
	if err := trySend(srv, listener, "orders"); !isUnauthorized(err) {
		t.Errorf("send with a Listen-only policy: %v, want unauthorized", err)
	}
	if err := trySend(srv, sender, "payments"); !isUnauthorized(err) {
		t.Errorf("send to another entity with an entity policy: %v, want unauthorized", err)
	}
	// Wrong key and unknown policy.
	if err := trySend(srv, withKey(root, "sender", "wrong"), "orders"); !isUnauthorized(err) {
		t.Errorf("send with a wrong key: %v, want unauthorized", err)
	}
	if err := trySend(srv, withKey(root, "ghost", "send-key"), "orders"); !isUnauthorized(err) {
		t.Errorf("send with an unknown policy: %v, want unauthorized", err)
	}

	// The administration client needs Manage.
	ctx := testContext(t)
	if _, err := newAdminClientFor(t, root).GetQueue(ctx, "orders", nil); err != nil {
		t.Errorf("admin with the root key: %v", err)
	}
	if _, err := newAdminClientFor(t, app).GetQueue(ctx, "orders", nil); responseStatus(err) != http.StatusUnauthorized {
		t.Errorf("admin with a policy without Manage: %v, want 401", err)
	}
	if _, err := newAdminClientFor(t, withKey(root, "ghost", "x")).GetQueue(ctx, "orders", nil); responseStatus(err) != http.StatusUnauthorized {
		t.Errorf("admin with an unknown policy: %v, want 401", err)
	}

	// Regenerating the key stops new connections that use the old one.
	if _, err := srv.Authority().RegenerateKey(ctx, "orders", "sender", false); err != nil {
		t.Fatal(err)
	}
	if err := trySend(srv, sender, "orders"); !isUnauthorized(err) {
		t.Errorf("send with a regenerated key: %v, want unauthorized", err)
	}
	p, _ := srv.Authority().Policy("orders", "sender")
	if err := trySend(srv, withKey(root, "sender", p.PrimaryKey), "orders"); err != nil {
		t.Errorf("send with the new key: %v", err)
	}
}

func TestLocalAuthDisabled(t *testing.T) {
	t.Parallel()
	srv := authServer(t, auth.ModeSAS, func(o *server.Options) { o.DisableLocalAuth = true })
	if err := trySend(srv, srv.ConnectionString(), "orders"); !isUnauthorized(err) {
		t.Errorf("send with SAS while it is disabled: %v, want unauthorized", err)
	}
}

func TestAuthObservabilityEndpoints(t *testing.T) {
	t.Parallel()
	srv := authServer(t, auth.ModeSAS, func(o *server.Options) { o.Prometheus = true })
	base := "http://" + srv.HTTPAddr().String()
	get := func(path, authorization string) int {
		req, _ := http.NewRequest("GET", base+path, nil)
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	token := auth.SASToken(base, auth.RootPolicyName, auth.RootKey, time.Now().Add(time.Hour))
	for _, path := range []string{"/$api/metrics", "/$api/activity", "/$metrics"} {
		if got := get(path, ""); got != http.StatusUnauthorized {
			t.Errorf("GET %s without a token = %d, want 401", path, got)
		}
		if got := get(path, token); got != http.StatusOK {
			t.Errorf("GET %s with the root token = %d, want 200", path, got)
		}
	}
	appToken := auth.SASToken(base, "app", "app-key", time.Now().Add(time.Hour))
	if got := get("/$api/metrics", appToken); got != http.StatusUnauthorized {
		t.Errorf("GET /$api/metrics without Manage = %d, want 401", got)
	}
}

func TestPortalLogin(t *testing.T) {
	t.Parallel()
	srv := authServer(t, auth.ModeSAS, nil)
	base := "http://" + srv.HTTPAddr().String()
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	resp, err := noRedirect.Get(base + "/$portal/queues")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/$portal/login" {
		t.Errorf("portal without a session = %d %q, want a redirect to the login", resp.StatusCode, resp.Header.Get("Location"))
	}
	// A policy without Manage cannot sign in; the root key can.
	login := func(form url.Values) *http.Response {
		req, _ := http.NewRequest("POST", base+"/$portal/login", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Origin", base)
		resp, err := noRedirect.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	if got := login(url.Values{"name": {"app"}, "key": {"app-key"}}).StatusCode; got != http.StatusUnauthorized {
		t.Errorf("login without Manage = %d, want 401", got)
	}
	if got := login(url.Values{"name": {auth.RootPolicyName}, "key": {"wrong"}}).StatusCode; got != http.StatusUnauthorized {
		t.Errorf("login with a wrong key = %d, want 401", got)
	}
	ok := login(url.Values{"connection": {srv.ConnectionString()}})
	if ok.StatusCode != http.StatusSeeOther {
		t.Fatalf("login with the connection string = %d", ok.StatusCode)
	}
	var cookie *http.Cookie
	for _, c := range ok.Cookies() {
		if c.Name == "osb_portal" {
			cookie = c
		}
	}
	if cookie == nil || !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode {
		t.Fatalf("session cookie = %+v", cookie)
	}
	req, _ := http.NewRequest("GET", base+"/$portal/queues", nil)
	req.AddCookie(cookie)
	resp, err = noRedirect.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("portal with a session = %d, want 200", resp.StatusCode)
	}
}

// devCredential is an azcore.TokenCredential that asks the emulator's
// development issuer for a token.
type devCredential struct {
	srv       *server.Server
	principal string
	ttl       time.Duration
}

func (c devCredential) GetToken(ctx context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	ttl := c.ttl
	if ttl == 0 {
		ttl = time.Hour
	}
	token, expires, err := c.srv.DevIssuer().Token(c.principal, ttl)
	return azcore.AccessToken{Token: token, ExpiresOn: expires}, err
}

// newJWTClient connects with an Entra ID credential. The emulator serves
// TLS on its port with a self-signed certificate.
func newJWTClient(t *testing.T, srv *server.Server, principal string) *azservicebus.Client {
	t.Helper()
	client, err := azservicebus.NewClient("localhost", devCredential{srv: srv, principal: principal}, &azservicebus.ClientOptions{
		TLSConfig:      &tls.Config{InsecureSkipVerify: true},
		CustomEndpoint: srv.Addr().String(),
		RetryOptions:   azservicebus.RetryOptions{MaxRetries: -1},
	})
	if err != nil {
		t.Fatalf("azservicebus.NewClient() error: %v", err)
	}
	t.Cleanup(func() { client.Close(context.Background()) })
	return client
}

func TestEntraRoles(t *testing.T) {
	t.Parallel()
	srv := authServer(t, auth.ModeFull, func(o *server.Options) {
		o.DevIssuer = true
		o.Assignments = []server.Assignment{
			{Principal: "owner", Role: auth.RoleOwner},
			{Principal: "sender", Role: auth.RoleSender, Scope: "orders"},
			{Principal: "receiver", Role: auth.RoleReceiver},
		}
	})
	ctx := testContext(t)
	send := func(client *azservicebus.Client, queue string) error {
		s, err := client.NewSender(queue, nil)
		if err != nil {
			return err
		}
		defer s.Close(ctx)
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return s.SendMessage(c, &azservicebus.Message{Body: []byte("x")}, nil)
	}
	peek := func(client *azservicebus.Client, queue string) error {
		r, err := client.NewReceiverForQueue(queue, nil)
		if err != nil {
			return err
		}
		defer r.Close(ctx)
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err = r.PeekMessages(c, 1, nil)
		return err
	}

	owner, sender, receiver, nobody := newJWTClient(t, srv, "owner"), newJWTClient(t, srv, "sender"), newJWTClient(t, srv, "receiver"), newJWTClient(t, srv, "nobody")
	if err := send(owner, "payments"); err != nil {
		t.Errorf("Data Owner send: %v", err)
	}
	if err := peek(owner, "payments"); err != nil {
		t.Errorf("Data Owner receive: %v", err)
	}
	if err := send(sender, "orders"); err != nil {
		t.Errorf("Data Sender send on its entity: %v", err)
	}
	if err := send(sender, "payments"); !isUnauthorized(err) {
		t.Errorf("Data Sender send on another entity: %v, want unauthorized", err)
	}
	if err := peek(sender, "orders"); !isUnauthorized(err) {
		t.Errorf("Data Sender receive: %v, want unauthorized", err)
	}
	if err := peek(receiver, "payments"); err != nil {
		t.Errorf("Data Receiver receive: %v", err)
	}
	if err := send(receiver, "payments"); !isUnauthorized(err) {
		t.Errorf("Data Receiver send: %v, want unauthorized", err)
	}
	if err := send(nobody, "orders"); !isUnauthorized(err) {
		t.Errorf("a principal without a role: %v, want unauthorized", err)
	}

	// An expired credential.
	expired, err := azservicebus.NewClient("localhost", devCredential{srv: srv, principal: "owner", ttl: -time.Minute}, &azservicebus.ClientOptions{
		TLSConfig: &tls.Config{InsecureSkipVerify: true}, CustomEndpoint: srv.Addr().String(),
		RetryOptions: azservicebus.RetryOptions{MaxRetries: -1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer expired.Close(ctx)
	if err := send(expired, "orders"); err == nil {
		t.Error("send with an expired token succeeded")
	}

	// SAS keeps working next to Entra ID in full mode.
	if err := trySend(srv, srv.ConnectionString(), "orders"); err != nil {
		t.Errorf("SAS in full mode: %v", err)
	}

	// The administration API takes a Bearer token with Manage.
	base := "http://" + srv.HTTPAddr().String()
	httpWith := func(principal string) int {
		token, _, _ := srv.DevIssuer().Token(principal, time.Hour)
		req, _ := http.NewRequest("GET", base+"/orders", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if got := httpWith("owner"); got != http.StatusOK {
		t.Errorf("admin with the Data Owner token = %d, want 200", got)
	}
	if got := httpWith("sender"); got != http.StatusUnauthorized {
		t.Errorf("admin with the Data Sender token = %d, want 401", got)
	}

	// The issuer publishes its key and signs tokens over HTTP.
	resp, err := http.Get(base + "/$auth/jwks.json")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("GET /$auth/jwks.json = %v, %v", resp, err)
	}
	resp.Body.Close()
	resp, err = http.PostForm(base+"/$auth/token", url.Values{"principal": {"owner"}, "ttl": {"60"}})
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("POST /$auth/token = %v, %v", resp, err)
	}
	resp.Body.Close()
}

func TestEntraOptionsNeedFullMode(t *testing.T) {
	t.Parallel()
	_, err := server.New(context.Background(), server.Options{Addr: "127.0.0.1:0", Auth: auth.ModeSAS, DevIssuer: true})
	if err == nil {
		t.Error("server.New() with a dev issuer outside full mode succeeded")
	}
}

func TestAuthorizationRulesThroughTheAdminAPI(t *testing.T) {
	t.Parallel()
	srv := authServer(t, auth.ModeSAS, func(o *server.Options) { o.Queues = nil })
	root := srv.ConnectionString()
	ac := newAdminClientFor(t, root)
	ctx := testContext(t)

	key := "key-from-admin"
	_, err := ac.CreateQueue(ctx, "jobs", &admin.CreateQueueOptions{Properties: &admin.QueueProperties{
		AuthorizationRules: []admin.AuthorizationRule{{
			KeyName:      to.Ptr("worker"),
			AccessRights: []admin.AccessRight{admin.AccessRightListen},
			PrimaryKey:   &key,
		}, {
			KeyName:      to.Ptr("producer"),
			AccessRights: []admin.AccessRight{admin.AccessRightSend},
		}},
	}})
	if err != nil {
		t.Fatalf("CreateQueue() with rules error: %v", err)
	}
	p, ok := srv.Authority().Policy("jobs", "worker")
	if !ok || p.Rights != auth.Listen || p.PrimaryKey != key {
		t.Fatalf("worker policy = %+v, %v", p, ok)
	}
	producer, ok := srv.Authority().Policy("jobs", "producer")
	if !ok || producer.PrimaryKey == "" {
		t.Fatalf("producer policy = %+v, %v (a key is generated when none is given)", producer, ok)
	}

	// The description lists the rules with their keys.
	got, err := ac.GetQueue(ctx, "jobs", nil)
	if err != nil || got == nil || len(got.AuthorizationRules) != 2 {
		t.Fatalf("GetQueue() = %+v, %v", got, err)
	}
	// The policies work.
	if err := trySend(srv, withKey(root, "producer", producer.PrimaryKey), "jobs"); err != nil {
		t.Errorf("send with the producer policy: %v", err)
	}
	if err := tryReceive(withKey(root, "worker", key), "jobs"); err != nil {
		t.Errorf("receive with the worker policy: %v", err)
	}

	// An update that leaves a rule out removes it; one that omits the
	// element altogether keeps them.
	got.AuthorizationRules = got.AuthorizationRules[:1]
	if _, err := ac.UpdateQueue(ctx, "jobs", got.QueueProperties, nil); err != nil {
		t.Fatalf("UpdateQueue() error: %v", err)
	}
	if n := len(srv.Authority().Policies("jobs", false)); n != 1 {
		t.Errorf("policies after dropping one = %d, want 1", n)
	}
	if _, err := ac.DeleteQueue(ctx, "jobs", nil); err != nil {
		t.Fatal(err)
	}
	if n := len(srv.Authority().Policies("jobs", false)); n != 0 {
		t.Errorf("policies of a deleted queue = %d, want 0", n)
	}
}
