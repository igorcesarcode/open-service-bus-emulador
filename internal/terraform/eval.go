package terraform

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/ext/typeexpr"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/convert"
	"github.com/zclconf/go-cty/cty/function"
	"github.com/zclconf/go-cty/cty/gocty"
)

// module is one Terraform module: the .tf files of a directory.
type module struct {
	dir string
	// path is the module address, such as "module.bus", or "" for the root.
	path      string
	variables map[string]*variable
	locals    map[string]*hclsyntax.Attribute
	resources []*resource
	modules   []*hclsyntax.Block

	localVals map[string]cty.Value
	funcs     map[string]function.Function
}

type variable struct {
	block    *hclsyntax.Block
	typ      cty.Type
	defaults *typeexpr.Defaults
	value    cty.Value
	set      bool
}

// resource is a resource block, expanded into instances by for_each or
// count.
type resource struct {
	typ, name string
	block     *hclsyntax.Block
	// expansion holds the last error evaluating for_each or count.
	expansion hcl.Diagnostics
	// expanded reports whether for_each or count is known.
	expanded  bool
	instances []*instance
	keyed     bool // for_each
	counted   bool // count
}

func (r *resource) address() string { return r.typ + "." + r.name }

// instance is one instance of a resource.
type instance struct {
	address string
	key     cty.Value // each.key, or a count index; NilVal when single
	value   cty.Value // each.value
	attrs   map[string]cty.Value
	diags   map[string]hcl.Diagnostics
	ctx     *hcl.EvalContext
}

// metaAttributes are resource arguments handled by Terraform itself.
var metaAttributes = map[string]bool{
	"count": true, "for_each": true, "depends_on": true, "provider": true,
}

// parseModule reads files, or every .tf file of dir when files is nil.
func parseModule(dir, path string, files []string) (*module, error) {
	if files == nil {
		var err error
		if files, err = tfFiles(dir); err != nil {
			return nil, err
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no .tf files in %s", dir)
	}
	sort.Strings(files)
	m := &module{
		dir:       dir,
		path:      path,
		variables: map[string]*variable{},
		locals:    map[string]*hclsyntax.Attribute{},
		localVals: map[string]cty.Value{},
		funcs:     functions(),
	}
	parser := hclparse.NewParser()
	for _, name := range files {
		f, diags, err := parseConfigFile(parser, name)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		if diags.HasErrors() {
			return nil, diags
		}
		body := f.Body.(*hclsyntax.Body)
		for _, b := range body.Blocks {
			if err := m.addBlock(b); err != nil {
				return nil, err
			}
		}
	}
	return m, nil
}

func (m *module) addBlock(b *hclsyntax.Block) error {
	switch b.Type {
	case "variable":
		v := &variable{block: b, typ: cty.DynamicPseudoType, value: cty.DynamicVal}
		if attr, ok := b.Body.Attributes["type"]; ok {
			typ, defaults, diags := typeexpr.TypeConstraintWithDefaults(attr.Expr)
			if diags.HasErrors() {
				return diags
			}
			v.typ, v.defaults = typ, defaults
		}
		m.variables[b.Labels[0]] = v
	case "locals":
		for name, attr := range b.Body.Attributes {
			m.locals[name] = attr
			m.localVals[name] = cty.DynamicVal
		}
	case "resource":
		m.resources = append(m.resources, &resource{typ: b.Labels[0], name: b.Labels[1], block: b})
	case "module":
		m.modules = append(m.modules, b)
	}
	// terraform, provider, data, output and other blocks do not declare
	// entities.
	return nil
}

// setVariable converts v to the type of variable name.
func (m *module) setVariable(name string, v cty.Value) error {
	vr, ok := m.variables[name]
	if !ok {
		return fmt.Errorf("value for undeclared variable %q", name)
	}
	if vr.defaults != nil {
		v = vr.defaults.Apply(v)
	}
	conv, err := convert.Convert(v, vr.typ)
	if err != nil {
		return fmt.Errorf("variable %q: %w", name, err)
	}
	vr.value, vr.set = conv, true
	return nil
}

// setDefaults gives variables without a value their default.
func (m *module) setDefaults() error {
	for name, vr := range m.variables {
		if vr.set {
			continue
		}
		attr, ok := vr.block.Body.Attributes["default"]
		if !ok {
			continue // Unknown: an error only if an entity needs it.
		}
		v, diags := attr.Expr.Value(nil)
		if diags.HasErrors() {
			return diags
		}
		if err := m.setVariable(name, v); err != nil {
			return err
		}
	}
	return nil
}

// parseVariableString converts a value given on the command line or in
// the environment: strings stay as they are, other types are parsed as
// HCL, as Terraform does.
func (m *module) parseVariableString(name, s string) (cty.Value, error) {
	vr, ok := m.variables[name]
	if !ok {
		return cty.NilVal, fmt.Errorf("value for undeclared variable %q", name)
	}
	if vr.typ == cty.String || vr.typ == cty.DynamicPseudoType {
		return cty.StringVal(s), nil
	}
	expr, diags := hclsyntax.ParseExpression([]byte(s), "-tf-var "+name, hcl.InitialPos)
	if diags.HasErrors() {
		return cty.NilVal, diags
	}
	v, diags := expr.Value(&hcl.EvalContext{Functions: m.funcs})
	if diags.HasErrors() {
		return cty.NilVal, diags
	}
	return v, nil
}

// loadVarFile reads a .tfvars file.
func (m *module) loadVarFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if strings.HasSuffix(path, ".json") {
		if data, err = convertJSON(data, true); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	f, diags := hclsyntax.ParseConfig(data, path, hcl.InitialPos)
	if diags.HasErrors() {
		return diags
	}
	attrs, diags := f.Body.JustAttributes()
	if diags.HasErrors() {
		return diags
	}
	for name, attr := range attrs {
		v, diags := attr.Expr.Value(&hcl.EvalContext{Functions: m.funcs})
		if diags.HasErrors() {
			return diags
		}
		if err := m.setVariable(name, v); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	return nil
}

// evaluate computes locals and resource attributes. References between
// them are resolved by repeating passes until no more values become
// known; values that depend on unknown variables stay unknown.
func (m *module) evaluate() {
	prev := -1
	for range 64 {
		ctx := m.context()
		for name, attr := range m.locals {
			if v, diags := attr.Expr.Value(ctx); !diags.HasErrors() {
				m.localVals[name] = v
			}
		}
		for _, r := range m.resources {
			m.expand(r, ctx)
		}
		known := m.knownCount()
		if known == prev {
			return
		}
		prev = known
	}
}

func (m *module) knownCount() int {
	n := 0
	for _, v := range m.localVals {
		if v.IsWhollyKnown() {
			n++
		}
	}
	for _, r := range m.resources {
		for _, in := range r.instances {
			for _, v := range in.attrs {
				if v.IsWhollyKnown() {
					n++
				}
			}
		}
	}
	return n
}

// context returns the evaluation context of the module's expressions.
func (m *module) context() *hcl.EvalContext {
	vars := map[string]cty.Value{}
	for name, v := range m.variables {
		vars[name] = v.value
	}
	byType := map[string]map[string]cty.Value{}
	for _, r := range m.resources {
		if byType[r.typ] == nil {
			byType[r.typ] = map[string]cty.Value{}
		}
		byType[r.typ][r.name] = r.ctyValue()
	}
	abs, _ := filepath.Abs(m.dir)
	ctx := &hcl.EvalContext{
		Variables: map[string]cty.Value{
			"var":   cty.ObjectVal(vars),
			"local": cty.ObjectVal(m.localVals),
			"path": cty.ObjectVal(map[string]cty.Value{
				"module": cty.StringVal(abs),
				"root":   cty.StringVal(abs),
				"cwd":    cty.StringVal(abs),
			}),
			"terraform": cty.ObjectVal(map[string]cty.Value{"workspace": cty.StringVal("default")}),
			// Data sources and module outputs are not read.
			"data":   cty.DynamicVal,
			"module": cty.DynamicVal,
		},
		Functions: m.funcs,
	}
	for typ, rs := range byType {
		ctx.Variables[typ] = cty.ObjectVal(rs)
	}
	return ctx
}

// ctyValue is the value other expressions see for the resource.
func (r *resource) ctyValue() cty.Value {
	if !r.expanded {
		return cty.DynamicVal
	}
	switch {
	case r.keyed:
		objs := map[string]cty.Value{}
		for _, in := range r.instances {
			objs[in.key.AsString()] = in.object()
		}
		return cty.ObjectVal(objs)
	case r.counted:
		objs := make([]cty.Value, len(r.instances))
		for i, in := range r.instances {
			objs[i] = in.object()
		}
		return cty.TupleVal(objs)
	case len(r.instances) == 1:
		return r.instances[0].object()
	}
	return cty.DynamicVal
}

func (in *instance) object() cty.Value {
	attrs := map[string]cty.Value{}
	for name, v := range in.attrs {
		attrs[name] = v
	}
	if _, ok := attrs["name"]; !ok {
		attrs["name"] = cty.DynamicVal
	}
	attrs["id"] = in.id()
	return cty.ObjectVal(attrs)
}

// expansionKey is one instance of a block with for_each or count.
type expansionKey struct{ key, value cty.Value }

// expansionKeys evaluates the for_each or count of body. A body with
// neither has one instance with no key. keyed and counted tell which one
// was found; diags is set when it is not known.
func expansionKeys(body *hclsyntax.Body, ctx *hcl.EvalContext) (keys []expansionKey, keyed, counted bool, diags hcl.Diagnostics) {
	if attr, ok := body.Attributes["for_each"]; ok {
		v, d := attr.Expr.Value(ctx)
		if d.HasErrors() || !v.IsWhollyKnown() || v.IsNull() {
			return nil, false, false, d
		}
		keys = []expansionKey{}
		switch {
		case v.Type().IsSetType():
			for it := v.ElementIterator(); it.Next(); {
				_, e := it.Element()
				if e.Type() != cty.String {
					return nil, false, false, diagf(attr.SrcRange, "for_each over a set needs strings")
				}
				keys = append(keys, expansionKey{e, e})
			}
		case v.Type().IsMapType() || v.Type().IsObjectType():
			for it := v.ElementIterator(); it.Next(); {
				k, e := it.Element()
				keys = append(keys, expansionKey{k, e})
			}
		default:
			return nil, false, false, diagf(attr.SrcRange, "for_each needs a map or a set of strings, got %s", v.Type().FriendlyName())
		}
		return keys, true, false, nil
	}
	if attr, ok := body.Attributes["count"]; ok {
		v, d := attr.Expr.Value(ctx)
		if d.HasErrors() || !v.IsWhollyKnown() || v.IsNull() {
			return nil, false, false, d
		}
		var n int
		if err := gocty.FromCtyValue(v, &n); err != nil {
			return nil, false, false, diagf(attr.SrcRange, "count: %v", err)
		}
		keys = []expansionKey{}
		for i := range n {
			keys = append(keys, expansionKey{key: cty.NumberIntVal(int64(i))})
		}
		return keys, false, true, nil
	}
	return []expansionKey{{}}, false, false, nil
}

// expansionContext returns a child of ctx that defines each or count for
// one instance, and the suffix of the instance's address.
func expansionContext(ctx *hcl.EvalContext, k expansionKey, keyed, counted bool) (*hcl.EvalContext, string) {
	ictx := ctx.NewChild()
	switch {
	case keyed:
		ictx.Variables = map[string]cty.Value{"each": cty.ObjectVal(map[string]cty.Value{"key": k.key, "value": k.value})}
		return ictx, fmt.Sprintf("[%q]", k.key.AsString())
	case counted:
		ictx.Variables = map[string]cty.Value{"count": cty.ObjectVal(map[string]cty.Value{"index": k.key})}
		return ictx, "[" + k.key.AsBigFloat().String() + "]"
	}
	return ictx, ""
}

// expand evaluates for_each or count and then every argument of each
// instance.
func (m *module) expand(r *resource, ctx *hcl.EvalContext) {
	body := r.block.Body
	r.expanded, r.keyed, r.counted, r.expansion = false, false, false, nil
	keys, keyed, counted, diags := expansionKeys(body, ctx)
	if keys == nil {
		r.expansion = diags
		return
	}
	r.keyed, r.counted = keyed, counted
	r.expanded = true

	r.instances = r.instances[:0]
	for _, k := range keys {
		in := &instance{address: r.address(), key: k.key, value: k.value, attrs: map[string]cty.Value{}, diags: map[string]hcl.Diagnostics{}}
		ictx, suffix := expansionContext(ctx, k, r.keyed, r.counted)
		in.address += suffix
		in.ctx = ictx
		for name, attr := range body.Attributes {
			if metaAttributes[name] {
				continue
			}
			v, diags := attr.Expr.Value(ictx)
			if diags.HasErrors() {
				in.diags[name] = diags
				v = cty.DynamicVal
			}
			in.attrs[name] = v
		}
		r.instances = append(r.instances, in)
	}
}

// id builds an Azure resource id, so that arguments such as topic_id can
// name their parent. Arguments of older providers (namespace_name,
// topic_name, subscription_name) are used when the id arguments are
// missing.
func (in *instance) id() cty.Value {
	typ, _, _ := strings.Cut(in.address, ".")
	name, ok := in.knownString("name")
	if !ok {
		return cty.DynamicVal
	}
	parent := func(idAttr, nameAttr, segment string, grand func() (string, bool)) (string, bool) {
		if id, ok := in.knownString(idAttr); ok {
			return id, true
		}
		if _, set := in.attrs[idAttr]; set {
			return "", false
		}
		if n, ok := in.knownString(nameAttr); ok {
			g, ok := grand()
			return g + "/" + segment + "/" + n, ok
		}
		return "", false
	}
	namespace := func() (string, bool) {
		_, byID := in.attrs["namespace_id"]
		_, byName := in.attrs["namespace_name"]
		if !byID && !byName {
			// The namespace is implied: the emulator serves one.
			return emulatorGroup + "/namespaces/emulator", true
		}
		return parent("namespace_id", "namespace_name", "namespaces", func() (string, bool) { return emulatorGroup, true })
	}
	topic := func() (string, bool) { return parent("topic_id", "topic_name", "topics", namespace) }
	var p string
	switch typ {
	case "azurerm_servicebus_namespace":
		rg, ok := in.knownString("resource_group_name")
		if !ok {
			rg = "emulator"
		}
		return cty.StringVal("/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/" + rg +
			"/providers/Microsoft.ServiceBus/namespaces/" + name)
	case "azurerm_servicebus_queue":
		p, ok = namespace()
		p += "/queues/"
	case "azurerm_servicebus_topic":
		p, ok = namespace()
		p += "/topics/"
	case "azurerm_servicebus_subscription":
		p, ok = topic()
		p += "/subscriptions/"
	case "azurerm_servicebus_subscription_rule":
		p, ok = parent("subscription_id", "subscription_name", "subscriptions", topic)
		p += "/rules/"
	default:
		p, ok = "/"+typ+"/", true
	}
	if !ok {
		return cty.DynamicVal
	}
	return cty.StringVal(p + name)
}

// emulatorGroup prefixes ids built from names.
const emulatorGroup = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/emulator/providers/Microsoft.ServiceBus"

func (in *instance) knownString(name string) (string, bool) {
	v, ok := in.attrs[name]
	if !ok || !v.IsWhollyKnown() || v.IsNull() {
		return "", false
	}
	v, err := convert.Convert(v, cty.String)
	if err != nil {
		return "", false
	}
	return v.AsString(), true
}

func diagf(rng hcl.Range, format string, args ...any) hcl.Diagnostics {
	return hcl.Diagnostics{{Severity: hcl.DiagError, Summary: fmt.Sprintf(format, args...), Subject: rng.Ptr()}}
}

// tfFiles lists the .tf and .tf.json files of dir.
func tfFiles(dir string) ([]string, error) {
	var files []string
	for _, pattern := range []string{"*.tf", "*.tf.json"} {
		m, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return nil, err
		}
		files = append(files, m...)
	}
	return files, nil
}

// parseConfigFile parses a .tf file, or a .tf.json one through its native
// syntax equivalent.
func parseConfigFile(parser *hclparse.Parser, name string) (*hcl.File, hcl.Diagnostics, error) {
	if !strings.HasSuffix(name, ".json") {
		f, diags := parser.ParseHCLFile(name)
		return f, diags, nil
	}
	data, err := os.ReadFile(name)
	if err != nil {
		return nil, nil, err
	}
	src, err := convertJSON(data, false)
	if err != nil {
		return nil, nil, err
	}
	f, diags := parser.ParseHCL(src, name)
	return f, diags, nil
}
