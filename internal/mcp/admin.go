package mcp

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

type (
	logLevelIn struct {
		Level string `json:"level" desc:"debug, info, warn or error." enum:"debug,info,warn,error" required:"true"`
	}
	scopeIn struct {
		Common
		Scope string `json:"scope,omitempty" desc:"Entity path whose policies to list; the namespace's when left out."`
		All   bool   `json:"all,omitempty" desc:"List the policies of every scope."`
	}
	policyIn struct {
		Common
		Name         string   `json:"name" desc:"Policy name." required:"true"`
		Scope        string   `json:"scope,omitempty" desc:"Entity path; the namespace when left out."`
		Rights       []string `json:"rights" desc:"Listen, Send and/or Manage." required:"true"`
		PrimaryKey   string   `json:"primary_key,omitempty" desc:"Generated when left out."`
		SecondaryKey string   `json:"secondary_key,omitempty" desc:"Generated when left out."`
	}
	policyRefIn struct {
		Common
		Name  string `json:"name" required:"true"`
		Scope string `json:"scope,omitempty" desc:"Entity path; the namespace when left out."`
	}
	sasTokenIn struct {
		Common
		Policy   string   `json:"policy" desc:"Policy name, such as RootManageSharedAccessKey." required:"true"`
		Scope    string   `json:"scope,omitempty" desc:"Scope of the policy: entity path; the namespace when left out."`
		Resource string   `json:"resource" desc:"URI the token is for, such as sb://localhost:5672/orders." required:"true"`
		TTL      Duration `json:"ttl,omitempty" desc:"Validity: ISO 8601 or seconds. Default PT1H."`
		Key      string   `json:"key,omitempty" desc:"Which key signs it. Default primary." enum:"primary,secondary"`
	}
	devTokenIn struct {
		Principal string   `json:"principal" desc:"Object id of the principal; role assignments name it." required:"true"`
		TTL       Duration `json:"ttl,omitempty" desc:"Validity: ISO 8601 or seconds. Default PT1H."`
	}
	geoPairIn struct {
		Alias     string `json:"alias" required:"true"`
		Primary   string `json:"primary" desc:"Namespace that serves the alias." required:"true"`
		Secondary string `json:"secondary" desc:"Namespace that receives a copy of the primary's entities." required:"true"`
	}
	geoAliasIn struct {
		Alias string `json:"alias" required:"true"`
	}
	geoFailoverIn struct {
		Alias string `json:"alias" required:"true"`
		Force bool   `json:"force,omitempty" desc:"Fail over without waiting for the secondary to catch up."`
	}
	geoEnableIn struct {
		Common
		Synchronous     bool     `json:"synchronous,omitempty" desc:"Commit every change in both regions before acknowledging it."`
		MaxLag          Duration `json:"max_lag,omitempty" desc:"Asynchronous mode: lag at which the primary throttles. Default PT1M."`
		Delay           Duration `json:"delay,omitempty" desc:"Asynchronous mode: simulated distance between the regions."`
		PrimaryRegion   string   `json:"primary_region,omitempty"`
		SecondaryRegion string   `json:"secondary_region,omitempty"`
	}
	geoPromoteIn struct {
		Common
		Forced bool `json:"forced,omitempty" desc:"Promote without waiting for the secondary to catch up; changes it lacks are lost."`
	}
)

func (s *Server) registerAdminTools() {
	em := s.opts.Emulator

	add(s, "list_namespaces", "Lists the namespaces of the emulator and the Geo-DR aliases.", read,
		func(ctx context.Context, _ struct{}) (any, error) {
			var names []string
			for i, n := range em.Namespaces() {
				if i == 0 {
					names = append(names, n.Name()+" (default)")
				} else {
					names = append(names, n.Name())
				}
			}
			return map[string]any{"namespaces": names, "geoDrAliases": geoAliases(em)}, nil
		})

	add(s, "get_server_status", "Reports the emulator process: version, addresses it listens on, uptime, TLS certificate, database file and the MCP settings.", read,
		func(ctx context.Context, _ struct{}) (any, error) {
			v := map[string]any{
				"version": em.Version(), "started": em.Started().UTC().Format(time.RFC3339),
				"uptime":   time.Since(em.Started()).Round(time.Second).String(),
				"mcp":      map[string]any{"readOnly": s.opts.ReadOnly, "allowData": s.opts.AllowData, "tools": len(s.tools)},
				"database": map[string]any{},
			}
			for k, a := range map[string]net.Addr{"address": em.Addr(), "amqpAddress": em.AMQPAddr(), "httpAddress": em.HTTPAddr()} {
				if a != nil {
					v[k] = a.String()
				}
			}
			if ma := s.Addr(); ma != nil {
				v["mcpAddress"] = ma.String()
			}
			n := em.Namespaces()[0]
			db := map[string]any{"path": n.DBPath()}
			if n.DBPath() != "in memory" {
				if st, err := os.Stat(n.DBPath()); err == nil {
					db["sizeBytes"] = st.Size()
				}
			}
			v["database"] = db
			if c := certInfo(em.CertificatePEM()); c != nil {
				v["certificate"] = c
			}
			return v, nil
		})

	add(s, "get_certificate", "Returns the TLS certificate of the emulator in PEM form, for clients to trust.", read,
		func(ctx context.Context, _ struct{}) (any, error) {
			pem := em.CertificatePEM()
			if pem == nil {
				return nil, fmt.Errorf("the server has no TLS certificate")
			}
			return map[string]any{"pem": string(pem), "details": certInfo(pem)}, nil
		})

	if s.opts.LogLevel != nil {
		add(s, "set_log_level", "Changes the level of the emulator's log while it runs.", write,
			func(ctx context.Context, in logLevelIn) (any, error) {
				var l slog.Level
				if err := l.UnmarshalText([]byte(in.Level)); err != nil {
					return nil, fmt.Errorf("level must be debug, info, warn or error")
				}
				s.opts.LogLevel.Set(l)
				return map[string]any{"level": l.String()}, nil
			})
	}

	add(s, "list_authorization_rules", "Lists the shared access policies of a namespace or an entity, with their keys. Anyone who reaches this port can read them.", read,
		func(ctx context.Context, in scopeIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			items := []any{}
			for _, p := range n.Authority().Policies(in.Scope, in.All) {
				items = append(items, policyView(p))
			}
			return map[string]any{"authMode": n.Authority().Mode(), "items": items}, nil
		})
	add(s, "create_sas_policy", "Creates or replaces a shared access policy.", write,
		func(ctx context.Context, in policyIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			rights, err := auth.ParseRights(in.Rights)
			if err != nil {
				return nil, err
			}
			p := auth.Policy{Scope: in.Scope, Name: in.Name, Rights: rights, PrimaryKey: in.PrimaryKey, SecondaryKey: in.SecondaryKey}
			if err := n.Authority().SavePolicy(ctx, p); err != nil {
				return nil, err
			}
			saved, _ := n.Authority().Policy(in.Scope, in.Name)
			return policyView(saved), nil
		})
	add(s, "delete_sas_policy", "Deletes a shared access policy. Tokens signed with it stop working.", write,
		func(ctx context.Context, in policyRefIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			if err := n.Authority().DeletePolicy(ctx, in.Scope, in.Name); err != nil {
				return nil, err
			}
			return map[string]any{"deleted": in.Name}, nil
		})
	add(s, "generate_sas_token", "Signs a shared access signature for a resource with a policy of the namespace, to try a client with.", read,
		func(ctx context.Context, in sasTokenIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			p, ok := n.Authority().Policy(in.Scope, in.Policy)
			if !ok {
				return nil, fmt.Errorf("policy %q not found", in.Policy)
			}
			ttl, err := durationArg(in.TTL, time.Hour)
			if err != nil {
				return nil, fmt.Errorf("ttl: %w", err)
			}
			key := p.PrimaryKey
			if in.Key == "secondary" {
				key = p.SecondaryKey
			}
			expires := time.Now().Add(ttl)
			return map[string]any{"token": auth.SASToken(in.Resource, p.Name, key, expires), "expires": expires.UTC().Format(time.RFC3339)}, nil
		})
	add(s, "issue_dev_token", "Signs an Entra ID style access token for a principal. Works only when the emulator runs with -auth full -dev-issuer.", read,
		func(ctx context.Context, in devTokenIn) (any, error) {
			d := em.DevIssuer()
			if d == nil {
				return nil, fmt.Errorf("the emulator was not started with -auth full -dev-issuer")
			}
			ttl, err := durationArg(in.TTL, time.Hour)
			if err != nil {
				return nil, fmt.Errorf("ttl: %w", err)
			}
			tok, exp, err := d.Token(in.Principal, ttl)
			if err != nil {
				return nil, err
			}
			return map[string]any{"access_token": tok, "token_type": "Bearer", "expires": exp.UTC().Format(time.RFC3339)}, nil
		})

	s.registerGeoTools()
}

func (s *Server) registerGeoTools() {
	em := s.opts.Emulator
	add(s, "geo_dr_status", "Lists the Geo-DR aliases: primary, secondary, pairing and last synchronization.", read,
		func(ctx context.Context, _ struct{}) (any, error) {
			return map[string]any{"items": geoAliases(em)}, nil
		})
	add(s, "geo_dr_pair", "Pairs two namespaces under an alias; the primary's entities and rules are copied to the secondary.", write,
		func(ctx context.Context, in geoPairIn) (any, error) {
			return ok(em.PairGeoDR(ctx, in.Alias, in.Primary, in.Secondary))
		})
	add(s, "geo_dr_failover", "Fails the alias over to its secondary, which becomes the primary. The pairing is broken.", write,
		func(ctx context.Context, in geoFailoverIn) (any, error) {
			return ok(em.FailoverGeoDR(ctx, in.Alias, in.Force))
		})
	add(s, "geo_dr_break", "Breaks the pairing of an alias, leaving it on its primary.", write,
		func(ctx context.Context, in geoAliasIn) (any, error) { return ok(em.BreakGeoDR(ctx, in.Alias)) })
	add(s, "geo_dr_delete_alias", "Deletes a Geo-DR alias.", write,
		func(ctx context.Context, in geoAliasIn) (any, error) { return ok(em.DeleteGeoDRAlias(ctx, in.Alias)) })

	add(s, "geo_replication_status", "Reports the Geo-Replication of a namespace: state, lag and last promotion.", read,
		func(ctx context.Context, in Common) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			st := n.GeoReplicationStatus()
			return map[string]any{
				"enabled": st.Enabled, "synchronous": st.Synchronous, "state": st.State,
				"primaryRegion": st.PrimaryRegion, "secondaryRegion": st.SecondaryRegion,
				"maxLag": st.MaxLag.String(), "delay": st.Delay.String(),
				"lagDuration": st.LagDuration.String(), "lagCount": st.LagCount,
				"promoting": st.Promoting, "lostChanges": st.LostChanges,
			}, nil
		})
	add(s, "geo_replication_enable", "Starts replicating a namespace's data to a second region.", write,
		func(ctx context.Context, in geoEnableIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			cfg := server.GeoReplication{Synchronous: in.Synchronous, PrimaryRegion: in.PrimaryRegion, SecondaryRegion: in.SecondaryRegion}
			if cfg.MaxLag, err = durationArg(in.MaxLag, 0); err != nil {
				return nil, fmt.Errorf("max_lag: %w", err)
			}
			if cfg.Delay, err = durationArg(in.Delay, 0); err != nil {
				return nil, fmt.Errorf("delay: %w", err)
			}
			return ok(n.EnableGeoReplication(ctx, cfg))
		})
	add(s, "geo_replication_disable", "Stops the Geo-Replication of a namespace.", write,
		func(ctx context.Context, in Common) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			return ok(n.DisableGeoReplication())
		})
	add(s, "geo_replication_promote", "Promotes the secondary region to primary.", write,
		func(ctx context.Context, in geoPromoteIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			return ok(n.PromoteGeoReplication(ctx, in.Forced))
		})
}

func ok(err error) (any, error) {
	if err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

func geoAliases(em *server.Server) []any {
	out := []any{}
	for _, a := range em.GeoDRAliases() {
		v := map[string]any{"alias": a.Alias, "primary": a.Primary, "secondary": a.Secondary, "paired": a.Paired}
		if !a.LastSync.IsZero() {
			v["lastSync"] = a.LastSync.UTC().Format(time.RFC3339)
		}
		if a.Error != "" {
			v["error"] = a.Error
		}
		out = append(out, v)
	}
	return out
}

func policyView(p auth.Policy) map[string]any {
	return map[string]any{
		"name": p.Name, "scope": p.Scope, "rights": p.Rights.Names(),
		"primaryKey": p.PrimaryKey, "secondaryKey": p.SecondaryKey,
	}
}

func certInfo(p []byte) map[string]any {
	b, _ := pem.Decode(p)
	if b == nil {
		return nil
	}
	c, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		return nil
	}
	hosts := append([]string{}, c.DNSNames...)
	for _, ip := range c.IPAddresses {
		hosts = append(hosts, ip.String())
	}
	return map[string]any{"subject": c.Subject.String(), "notAfter": c.NotAfter.UTC().Format(time.RFC3339), "hosts": strings.Join(hosts, ", ")}
}
