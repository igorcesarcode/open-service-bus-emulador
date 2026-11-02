package amqpsb

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
)

// claimsKey is where a connection keeps its claims in amqp.ConnData.
type claimsKey struct{}

// connClaims holds the claims a connection has presented, by the audience
// of the put-token request that carried them. A new token for an audience
// replaces the old one.
type connClaims struct {
	mu     sync.Mutex
	claims map[string]*auth.Claims
}

func (c *connClaims) put(audience string, cl *auth.Claims) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.claims == nil {
		c.claims = map[string]*auth.Claims{}
	}
	c.claims[audience] = cl
}

func (c *connClaims) allows(path string, right auth.Right, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cl := range c.claims {
		if cl.Allows(path, right, now) {
			return true
		}
	}
	return false
}

func (c *connClaims) allowsAny(path string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cl := range c.claims {
		if cl.AllowsAny(path, now) {
			return true
		}
	}
	return false
}

func (h *Handler) claimsOf(ctx context.Context) *connClaims {
	data := amqp.ConnData(ctx)
	if data == nil {
		return &connClaims{}
	}
	v, _ := data.LoadOrStore(claimsKey{}, &connClaims{})
	return v.(*connClaims)
}

// authorized reports whether the connection may use right on the entity at
// path. Without authentication everything is allowed.
func (h *Handler) authorized(ctx context.Context, path string, right auth.Right) bool {
	if !h.Auth.Enabled() {
		return true
	}
	return h.claimsOf(ctx).allows(path, right, time.Now())
}

func (h *Handler) authorizedAny(ctx context.Context, path string) bool {
	if !h.Auth.Enabled() {
		return true
	}
	return h.claimsOf(ctx).allowsAny(path, time.Now())
}

// unauthorized is the error Service Bus answers when a link needs a claim
// the connection does not have.
func unauthorized(resource, claims string) error {
	return amqp.NewError(amqp.CondUnauthorizedAccess, fmt.Sprintf(
		"Unauthorized access. '%s' claim(s) are required to perform this operation. Resource: '%s'.", claims, resource))
}

// managementRight is the right a $management operation needs: sending and
// scheduling need Send, rule operations need Manage, and the rest, which
// work on received messages and sessions, need Listen.
func managementRight(op string) auth.Right {
	switch strings.TrimPrefix(op, "com.microsoft:") {
	case "schedule-message", "cancel-scheduled-message":
		return auth.Send
	case "add-rule", "remove-rule", "enumerate-rules":
		return auth.Manage
	}
	return auth.Listen
}
