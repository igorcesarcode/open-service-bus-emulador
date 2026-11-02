package mcp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/config"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/terraform"
)

type (
	exportIn struct {
		Format string `json:"format,omitempty" desc:"Which file: config_json (the Config.json of the official emulator), terraform (main.tf) or both (default)." enum:"config_json,terraform,both"`
	}
	declIn struct {
		Common
		Format          string            `json:"format" desc:"What content holds: config_json, a Config.json of the official emulator, or terraform, Terraform with azurerm_servicebus_* resources." enum:"config_json,terraform" required:"true"`
		Content         string            `json:"content,omitempty" desc:"The Config.json, or the main.tf."`
		Files           map[string]string `json:"files,omitempty" desc:"Terraform only: several files by relative path, such as main.tf and the local modules they call, instead of content."`
		Variables       map[string]string `json:"variables,omitempty" desc:"Terraform only: root variables, as terraform -var does."`
		ModuleMap       string            `json:"module_map,omitempty" desc:"Terraform only: JSON naming the entity list arguments of modules whose source is not available."`
		DropDefaultRule bool              `json:"drop_default_rule,omitempty" desc:"Terraform only: remove the $Default rule of subscriptions that declare rules."`
		Prune           bool              `json:"prune,omitempty" desc:"Also delete the queues, topics and subscriptions that content does not declare."`
	}
	applyIn struct {
		declIn
		Confirm bool `json:"confirm,omitempty" desc:"Must be true when prune is set: pruning deletes entities and their messages for good."`
	}
	resetIn struct {
		Common
		Confirm
		Restore *bool `json:"restore_initial_config,omitempty" desc:"After deleting, declare again what the server started with (-config, -terraform, -queue...). Default true; false leaves the namespace empty."`
	}
	lockIn struct {
		Common
		Name  string `json:"name" desc:"Lock name." required:"true"`
		Level string `json:"level" desc:"CanNotDelete stops deletions; ReadOnly stops every change of entities and rules." enum:"CanNotDelete,ReadOnly" required:"true"`
		Scope string `json:"scope,omitempty" desc:"Entity path the lock covers; the whole namespace when left out."`
		Notes string `json:"notes,omitempty"`
	}
	deleteLockIn struct {
		Common
		Name string `json:"name" required:"true"`
	}
	tagsIn struct {
		Common
		Tags map[string]string `json:"tags" desc:"Replaces the tags of the namespace." required:"true"`
	}
)

func (s *Server) registerConfigTools() {
	add(s, "export_config", "Writes the current entities, rules, policies, locks and Geo-DR pairings of every namespace as Config.json and Terraform, ready to start an emulator again or to compare with production. Messages are not included.", read,
		func(ctx context.Context, in exportIn) (any, error) {
			files, err := s.opts.Emulator.Export()
			if err != nil {
				return nil, err
			}
			out := map[string]any{}
			for _, f := range files {
				switch {
				case f.Name == "Config.json" && in.Format != "terraform":
					out["config_json"] = string(f.Data)
				case f.Name == "main.tf" && in.Format != "config_json":
					out["terraform"] = string(f.Data)
				}
			}
			if len(out) == 0 {
				return nil, fmt.Errorf("the server was started without export support")
			}
			return out, nil
		})

	add(s, "diff_config", "Compares a Config.json or Terraform with a namespace and lists what apply_config would create, update or (with prune) delete, without changing anything.", read,
		func(ctx context.Context, in declIn) (any, error) {
			return s.applyConfig(ctx, in, true)
		})

	add(s, "apply_config", "Makes a namespace hold the entities and rules a Config.json or Terraform declares: creates what is missing and updates what differs, as terraform apply would. Messages of existing entities are kept. Entities that are not declared are kept too, unless prune is set, which deletes them and their messages. Tags, locks and policies in the content are not applied.", destroy,
		func(ctx context.Context, in applyIn) (any, error) {
			if in.Prune && !in.Confirm {
				return nil, errors.New("prune deletes entities and their messages for good; call it again with confirm=true")
			}
			return s.applyConfig(ctx, in.declIn, false)
		})

	add(s, "reset_namespace", "Deletes every queue, topic, subscription and message of a namespace, then declares again what the server started with. Meant to give a test a clean slate. Metrics and the activity log stay.", destroy,
		func(ctx context.Context, in resetIn) (any, error) {
			if err := in.check(); err != nil {
				return nil, err
			}
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			restore := in.Restore == nil || *in.Restore
			if err := n.Reset(ctx, restore); err != nil {
				return nil, err
			}
			return map[string]any{
				"namespace": n.Name(), "restored": restore,
				"queues": len(n.Broker().Entities(broker.KindQueue, "")), "topics": len(n.Broker().Entities(broker.KindTopic, "")),
			}, nil
		})

	add(s, "list_locks", "Lists the resource locks and tags of a namespace.", read,
		func(ctx context.Context, in Common) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			m := n.Broker().Meta()
			return map[string]any{"locks": nonNil(m.Locks), "tags": m.Tags, "readOnly": n.Broker().ReadOnly()}, nil
		})
	add(s, "set_lock", "Adds or replaces a resource lock, like an Azure management lock: ReadOnly stops every change of entities and rules, CanNotDelete stops deletions. Sending and receiving messages is never stopped.", write,
		func(ctx context.Context, in lockIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			l := broker.Lock{Name: in.Name, Level: broker.LockLevel(in.Level), Scope: in.Scope, Notes: in.Notes}
			if l.Level != broker.LockReadOnly && l.Level != broker.LockCanNotDelete {
				return nil, fmt.Errorf("level must be CanNotDelete or ReadOnly")
			}
			if err := n.Broker().SetLock(ctx, l); err != nil {
				return nil, err
			}
			return l, nil
		})
	add(s, "delete_lock", "Removes a resource lock.", write,
		func(ctx context.Context, in deleteLockIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			if err := n.Broker().DeleteLock(ctx, in.Name); err != nil {
				return nil, err
			}
			return map[string]any{"deleted": in.Name}, nil
		})
	add(s, "set_tags", "Replaces the tags of a namespace. They are kept for display and export only.", write,
		func(ctx context.Context, in tagsIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			if err := n.Broker().SetTags(ctx, in.Tags); err != nil {
				return nil, err
			}
			return map[string]any{"tags": in.Tags}, nil
		})
}

// applyConfig reads the declaration in in and applies it to the namespace
// it names, or only plans the changes.
func (s *Server) applyConfig(ctx context.Context, in declIn, dryRun bool) (any, error) {
	n, err := s.namespace(in.Namespace)
	if err != nil {
		return nil, err
	}
	cfg, warnings, err := loadDeclaration(in)
	if err != nil {
		return nil, err
	}
	target := cfg
	if len(cfg.Namespaces) > 0 {
		// A content that declares several namespaces applies the one that
		// is n's, with whatever it declares outside any namespace. Pruning
		// against a namespace it does not mention would delete everything.
		found := false
		for _, o := range cfg.Namespaces {
			found = found || o.Name == "" || strings.EqualFold(o.Name, n.Name())
		}
		if !found {
			return nil, fmt.Errorf("the content declares no namespace called %s", n.Name())
		}
		target, _ = cfg.Split(n.Name())
	}
	changes, err := n.Apply(ctx, target.Queues, target.Topics, in.Prune, dryRun)
	out := map[string]any{"namespace": n.Name(), "dryRun": dryRun, "changes": changeViews(changes), "warnings": nonNil(warnings)}
	for _, f := range target.Unsupported {
		out["warnings"] = append(out["warnings"].([]string), f.String())
	}
	if err != nil {
		return out, fmt.Errorf("%w (done so far: %s)", err, summarize(changes))
	}
	out["summary"] = summarize(changes)
	return out, nil
}

func changeViews(cs []server.Change) []any {
	out := make([]any, 0, len(cs))
	for _, c := range cs {
		v := map[string]any{"action": c.Action, "kind": c.Kind, "path": c.Path}
		if c.Detail != "" {
			v["detail"] = c.Detail
		}
		out = append(out, v)
	}
	return out
}

func summarize(cs []server.Change) string {
	counts := map[string]int{}
	for _, c := range cs {
		counts[c.Action]++
	}
	var parts []string
	for a, n := range counts {
		parts = append(parts, fmt.Sprintf("%d %s", n, a))
	}
	sort.Strings(parts)
	if len(parts) == 0 {
		return "nothing declared"
	}
	return strings.Join(parts, ", ")
}

// loadDeclaration parses a Config.json or Terraform given as text.
func loadDeclaration(in declIn) (*config.Config, []string, error) {
	switch in.Format {
	case "config_json":
		if in.Content == "" {
			return nil, nil, fmt.Errorf("content is empty")
		}
		c, err := config.LoadJSON([]byte(in.Content))
		return c, nil, err
	case "terraform":
		dir, err := os.MkdirTemp("", "osb-mcp-tf-")
		if err != nil {
			return nil, nil, err
		}
		defer os.RemoveAll(dir)
		files := in.Files
		if in.Content != "" {
			files = map[string]string{"main.tf": in.Content}
		}
		if len(files) == 0 {
			return nil, nil, fmt.Errorf("give content or files")
		}
		for name, data := range files {
			p := filepath.Join(dir, filepath.FromSlash(name))
			if rel, err := filepath.Rel(dir, p); err != nil || strings.HasPrefix(rel, "..") {
				return nil, nil, fmt.Errorf("file %q is outside the directory", name)
			}
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				return nil, nil, err
			}
			if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
				return nil, nil, err
			}
		}
		// The content comes over the network: it must not read this machine.
		opts := terraform.Options{Vars: in.Variables, DropDefaultRule: in.DropDefaultRule, Sandbox: true}
		if in.ModuleMap != "" {
			mp := filepath.Join(dir, ".module-map.json")
			if err := os.WriteFile(mp, []byte(in.ModuleMap), 0o644); err != nil {
				return nil, nil, err
			}
			if opts.ModuleMap, err = terraform.LoadModuleMap(mp); err != nil {
				return nil, nil, fmt.Errorf("module_map: %w", err)
			}
		}
		r, err := terraform.Load(dir, opts)
		if err != nil {
			return nil, nil, err
		}
		return r.Config, r.Warnings, nil
	}
	return nil, nil, fmt.Errorf("format must be config_json or terraform")
}
