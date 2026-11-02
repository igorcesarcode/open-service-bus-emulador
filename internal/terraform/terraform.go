// Package terraform declares emulator entities from the Terraform
// configuration of a production Service Bus namespace.
//
// The .tf files are read and evaluated without running Terraform:
// variables, locals, for_each, count, references between resources and
// the common built-in functions are supported. The azurerm_servicebus_queue,
// _topic, _subscription and _subscription_rule resources become entities
// with every argument applied; other resources are ignored.
//
// Modules are evaluated when their source is available: a local path, or a
// module downloaded by terraform init into .terraform/modules. A module
// whose source is not available is read by an adapter that recognizes
// lists of entities in its arguments (see adaptModule).
package terraform

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/config"
)

// Options configures Load.
type Options struct {
	// Vars sets root variables, as terraform -var name=value does.
	Vars map[string]string
	// VarFiles lists .tfvars files read after terraform.tfvars and
	// *.auto.tfvars.
	VarFiles []string
	// DropDefaultRule removes the $Default rule of subscriptions that
	// declare rules. Azure keeps it, so such subscriptions receive every
	// message unless $Default is deleted.
	DropDefaultRule bool
	// ModuleMap names the entity list arguments of modules whose source is
	// not available. See LoadModuleMap.
	ModuleMap map[string]ModuleLists
	// Sandbox is for a configuration that came from somewhere that should
	// not read this machine's files: local module sources must stay inside
	// the directory, and TF_VAR_ environment variables are ignored.
	Sandbox bool
}

// Result holds the declared entities and the warnings found reading them.
type Result struct {
	Config   *config.Config
	Warnings []string
}

// maxModuleDepth bounds nested module calls.
const maxModuleDepth = 16

// Load reads the Terraform configuration in dir, or in the single file
// dir names.
func Load(dir string, opts Options) (*Result, error) {
	root := dir
	if fi, err := os.Stat(dir); err != nil {
		return nil, err
	} else if !fi.IsDir() {
		root = filepath.Dir(dir)
	}
	m, err := parseFiles(dir, "")
	if err != nil {
		return nil, err
	}
	if err := setRootVariables(m, root, opts); err != nil {
		return nil, err
	}
	b := newBuilder(opts)
	if err := load(b, m, root, 0); err != nil {
		return nil, err
	}
	c, err := b.config()
	if err != nil {
		return nil, err
	}
	return &Result{Config: c, Warnings: b.warnings}, nil
}

func parseFiles(path, address string) (*module, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return parseModule(path, address, nil)
	}
	return parseModule(filepath.Dir(path), address, []string{path})
}

// setRootVariables applies, in Terraform's order of precedence, the
// defaults, TF_VAR_ environment variables, terraform.tfvars, *.auto.tfvars,
// opts.VarFiles and opts.Vars.
func setRootVariables(m *module, root string, opts Options) error {
	if err := m.setDefaults(); err != nil {
		return err
	}
	environ := os.Environ()
	if opts.Sandbox {
		environ = nil
	}
	for _, kv := range environ {
		k, v, _ := strings.Cut(kv, "=")
		name, ok := strings.CutPrefix(k, "TF_VAR_")
		if !ok || m.variables[name] == nil {
			continue
		}
		val, err := m.parseVariableString(name, v)
		if err != nil {
			return fmt.Errorf("%s: %w", k, err)
		}
		if err := m.setVariable(name, val); err != nil {
			return err
		}
	}
	files := []string{}
	for _, name := range []string{"terraform.tfvars", "terraform.tfvars.json"} {
		if _, err := os.Stat(filepath.Join(root, name)); err == nil {
			files = append(files, filepath.Join(root, name))
		}
	}
	var auto []string
	for _, pattern := range []string{"*.auto.tfvars", "*.auto.tfvars.json"} {
		m, _ := filepath.Glob(filepath.Join(root, pattern))
		auto = append(auto, m...)
	}
	sort.Strings(auto)
	files = append(append(files, auto...), opts.VarFiles...)
	for _, f := range files {
		if err := m.loadVarFile(f); err != nil {
			return err
		}
	}
	names := make([]string, 0, len(opts.Vars))
	for name := range opts.Vars {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		val, err := m.parseVariableString(name, opts.Vars[name])
		if err != nil {
			return err
		}
		if err := m.setVariable(name, val); err != nil {
			return err
		}
	}
	return nil
}

// load evaluates m and adds its entities and those of its modules.
func load(b *builder, m *module, root string, depth int) error {
	if depth > maxModuleDepth {
		return fmt.Errorf("%s: modules nested deeper than %d", m.path, maxModuleDepth)
	}
	m.evaluate()
	m.entities(b)
	ctx := m.context()
	for _, blk := range m.modules {
		base := m.prefix() + "module." + blk.Labels[0]
		keys, keyed, counted, diags := expansionKeys(blk.Body, ctx)
		if keys == nil {
			b.errorf("%s: for_each or count is not known; set the variables it depends on with -tf-var%s", base, detail(diags))
			continue
		}
		for _, k := range keys {
			ictx, suffix := expansionContext(ctx, k, keyed, counted)
			if err := loadModuleCall(b, m, root, depth, blk, base, base+suffix, ictx); err != nil {
				return err
			}
		}
	}
	return nil
}

// loadModuleCall reads one instance of a module call.
func loadModuleCall(b *builder, m *module, root string, depth int, blk *hclsyntax.Block, base, address string, ctx *hcl.EvalContext) error {
	dir, err := moduleDir(m, root, blk, base)
	if err != nil {
		return err
	}
	if dir != "" && b.opts.Sandbox && !within(root, dir) {
		return fmt.Errorf("%s: the module source is outside the configuration's directory", address)
	}
	if dir == "" {
		b.enter("") // An adapted module names no namespace.
		if !adaptModule(b, blk, ctx, address) {
			b.warnf("%s: the module source is not available and the module declares no known entity lists; run terraform init to download it", address)
		}
		return nil
	}
	child, err := parseModule(dir, address, nil)
	if err != nil {
		return fmt.Errorf("%s: %w", address, err)
	}
	if err := child.setDefaults(); err != nil {
		return fmt.Errorf("%s: %w", address, err)
	}
	for name, attr := range blk.Body.Attributes {
		if moduleMeta[name] {
			continue
		}
		v, diags := attr.Expr.Value(ctx)
		if diags.HasErrors() {
			v = cty.DynamicVal // An error only if an entity needs it.
		}
		if child.variables[name] == nil {
			b.warnf("%s: argument %s is not a variable of the module", address, name)
			continue
		}
		if err := child.setVariable(name, v); err != nil {
			return fmt.Errorf("%s: %w", address, err)
		}
	}
	return load(b, child, root, depth+1)
}

// indexSuffix matches the [key] or [0] of a module instance address.
var indexSuffix = regexp.MustCompile(`\[[^\]]*\]`)

var moduleMeta = map[string]bool{
	"source": true, "version": true, "providers": true, "count": true, "for_each": true, "depends_on": true,
}

// within reports whether dir is root or below it.
func within(root, dir string) bool {
	rel, err := filepath.Rel(root, dir)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// moduleDir returns the directory of a module call, or "" when its source
// is not available locally.
func moduleDir(m *module, root string, blk *hclsyntax.Block, address string) (string, error) {
	attr, ok := blk.Body.Attributes["source"]
	if !ok {
		return "", fmt.Errorf("%s: source is required", address)
	}
	v, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || v.Type() != cty.String {
		return "", fmt.Errorf("%s: source must be a literal string", address)
	}
	source := v.AsString()
	if strings.HasPrefix(source, "./") || strings.HasPrefix(source, "../") {
		return filepath.Join(m.dir, source), nil
	}
	// terraform init records downloaded modules by key, such as "bus" or
	// "bus.child" for nested calls.
	data, err := os.ReadFile(filepath.Join(root, ".terraform", "modules", "modules.json"))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var manifest struct {
		Modules []struct{ Key, Dir string }
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return "", fmt.Errorf("reading .terraform/modules/modules.json: %w", err)
	}
	key := indexSuffix.ReplaceAllString(strings.TrimPrefix(address, "module."), "")
	key = strings.ReplaceAll(key, ".module.", ".")
	for _, mod := range manifest.Modules {
		if mod.Key == key {
			return filepath.Join(root, mod.Dir), nil
		}
	}
	return "", nil
}
