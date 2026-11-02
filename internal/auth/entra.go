package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Audience is the audience of Entra ID tokens for Service Bus.
const Audience = "https://servicebus.azure.net"

// Entra validates Entra ID tokens: RS256 JWTs signed by keys published as a
// JWKS, with the Service Bus audience and the configured issuer.
type Entra struct {
	// Issuer is the expected iss claim. Empty accepts any issuer.
	Issuer string
	// Keys returns the signing keys by key id.
	Keys func(ctx context.Context) (map[string]*rsa.PublicKey, error)
	// Leeway tolerates clock differences in exp and nbf.
	Leeway time.Duration
}

type jwtClaims struct {
	Issuer    string          `json:"iss"`
	Audience  json.RawMessage `json:"aud"`
	Expires   float64         `json:"exp"`
	NotBefore float64         `json:"nbf"`
	OID       string          `json:"oid"`
	Subject   string          `json:"sub"`
	AppID     string          `json:"appid"`
}

func (c jwtClaims) hasAudience(want string) bool {
	var one string
	if json.Unmarshal(c.Audience, &one) == nil {
		return strings.TrimSuffix(one, "/") == want
	}
	var many []string
	if json.Unmarshal(c.Audience, &many) == nil {
		for _, a := range many {
			if strings.TrimSuffix(a, "/") == want {
				return true
			}
		}
	}
	return false
}

func b64(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
}

// validate checks a JWT and returns its principal (the oid claim, or the
// subject) and expiry.
func (e *Entra) validate(ctx context.Context, token string, now time.Time) (string, time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", time.Time{}, errors.New("not a JWT")
	}
	hdrRaw, err1 := b64(parts[0])
	claimsRaw, err2 := b64(parts[1])
	sig, err3 := b64(parts[2])
	if err1 != nil || err2 != nil || err3 != nil {
		return "", time.Time{}, errors.New("malformed JWT")
	}
	var hdr struct{ Alg, Kid string }
	if err := json.Unmarshal(hdrRaw, &hdr); err != nil || hdr.Alg != "RS256" {
		return "", time.Time{}, fmt.Errorf("unsupported JWT algorithm %q: want RS256", hdr.Alg)
	}
	keys, err := e.Keys(ctx)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("fetching the signing keys: %w", err)
	}
	key := keys[hdr.Kid]
	if key == nil && hdr.Kid == "" && len(keys) == 1 {
		for _, k := range keys {
			key = k
		}
	}
	if key == nil {
		return "", time.Time{}, fmt.Errorf("unknown signing key %q", hdr.Kid)
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig); err != nil {
		return "", time.Time{}, errors.New("invalid JWT signature")
	}
	var c jwtClaims
	if err := json.Unmarshal(claimsRaw, &c); err != nil {
		return "", time.Time{}, errors.New("malformed JWT claims")
	}
	if e.Issuer != "" && c.Issuer != e.Issuer {
		return "", time.Time{}, fmt.Errorf("unexpected issuer %q", c.Issuer)
	}
	if !c.hasAudience(Audience) {
		return "", time.Time{}, fmt.Errorf("the audience must be %s", Audience)
	}
	expires := time.Unix(int64(c.Expires), 0)
	if c.Expires == 0 || !now.Add(-e.Leeway).Before(expires) {
		return "", time.Time{}, ErrExpiredToken
	}
	if c.NotBefore != 0 && now.Add(e.Leeway).Before(time.Unix(int64(c.NotBefore), 0)) {
		return "", time.Time{}, errors.New("the token is not valid yet")
	}
	principal := c.OID
	if principal == "" {
		principal = c.Subject
	}
	if principal == "" {
		principal = c.AppID
	}
	if principal == "" {
		return "", time.Time{}, errors.New("the token names no principal (oid, sub or appid)")
	}
	return principal, expires, nil
}

// authenticateJWT validates an Entra ID token and gives it the grants of
// its principal's role assignments.
func (a *Authority) authenticateJWT(ctx context.Context, token string) (*Claims, error) {
	principal, expires, err := a.opts.Entra.validate(ctx, token, a.now())
	if err != nil {
		if errors.Is(err, ErrExpiredToken) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	return &Claims{Grants: a.grantsFor(principal), Expires: expires, Principal: principal, Source: "jwt"}, nil
}

// NewJWKS returns an Entra that reads its keys from a JWKS URL, caching
// them for ten minutes and refetching when a key is unknown.
func NewJWKS(issuer, url string, client *http.Client) *Entra {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	var (
		mu      sync.Mutex
		keys    map[string]*rsa.PublicKey
		fetched time.Time
	)
	return &Entra{
		Issuer: issuer, Leeway: time.Minute,
		Keys: func(ctx context.Context) (map[string]*rsa.PublicKey, error) {
			mu.Lock()
			defer mu.Unlock()
			if keys != nil && time.Since(fetched) < 10*time.Minute {
				return keys, nil
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return nil, err
			}
			resp, err := client.Do(req)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
			}
			data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			if err != nil {
				return nil, err
			}
			k, err := parseJWKS(data)
			if err != nil {
				return nil, err
			}
			keys, fetched = k, time.Now()
			return keys, nil
		},
	}
}

func parseJWKS(data []byte) (map[string]*rsa.PublicKey, error) {
	var doc struct {
		Keys []struct {
			Kty, Kid, N, E, Use string
		}
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parsing the JWKS: %w", err)
	}
	out := map[string]*rsa.PublicKey{}
	for _, k := range doc.Keys {
		if k.Kty != "RSA" || (k.Use != "" && k.Use != "sig") {
			continue
		}
		n, err1 := b64(k.N)
		e, err2 := b64(k.E)
		if err1 != nil || err2 != nil {
			continue
		}
		out[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
	}
	if len(out) == 0 {
		return nil, errors.New("the JWKS has no RSA signing keys")
	}
	return out, nil
}

// DevIssuer is a token issuer for tests without Entra ID: it publishes its
// public key as a JWKS and signs tokens for any principal on request.
type DevIssuer struct {
	issuer string
	kid    string
	key    *rsa.PrivateKey
	now    func() time.Time
}

// NewDevIssuer creates an issuer with a new key.
func NewDevIssuer(issuer string) (*DevIssuer, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	return &DevIssuer{issuer: issuer, kid: "open-service-bus-dev", key: key, now: time.Now}, nil
}

// Issuer is the iss claim of the tokens.
func (d *DevIssuer) Issuer() string { return d.issuer }

// Entra returns the validator for this issuer's tokens.
func (d *DevIssuer) Entra() *Entra {
	return &Entra{
		Issuer: d.issuer, Leeway: time.Minute,
		Keys: func(context.Context) (map[string]*rsa.PublicKey, error) {
			return map[string]*rsa.PublicKey{d.kid: &d.key.PublicKey}, nil
		},
	}
}

// JWKS returns the public key as a JSON Web Key Set.
func (d *DevIssuer) JWKS() []byte {
	e := big.NewInt(int64(d.key.E)).Bytes()
	out, _ := json.Marshal(map[string]any{"keys": []map[string]string{{
		"kty": "RSA", "use": "sig", "alg": "RS256", "kid": d.kid,
		"n": base64.RawURLEncoding.EncodeToString(d.key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(e),
	}}})
	return out
}

// Token signs a token for the principal (the oid claim), valid for ttl.
func (d *DevIssuer) Token(principal string, ttl time.Duration) (string, time.Time, error) {
	now := d.now()
	expires := now.Add(ttl)
	hdr, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": d.kid})
	claims, _ := json.Marshal(map[string]any{
		"iss": d.issuer, "aud": Audience, "oid": principal, "sub": principal,
		"iat": now.Unix(), "nbf": now.Unix(), "exp": expires.Unix(),
	})
	signing := base64.RawURLEncoding.EncodeToString(hdr) + "." + base64.RawURLEncoding.EncodeToString(claims)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, d.key, crypto.SHA256, sum[:])
	if err != nil {
		return "", time.Time{}, err
	}
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig), expires, nil
}
