package terraform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/config"
)

// Arguments of modules whose source is not available that list entities,
// as in:
//
//	module "bus" {
//	  asb_queues      = [{ name = "jobs" }]
//	  asb_topics_name = [{ name = "orders" }]
//	  asb_subs        = [{ name = "audit", topic = "orders", sql_filter = "event = 'x'" }]
//	}
//
// Elements take the arguments of the azurerm resources as properties,
// such as lock_duration and max_delivery_count. A subscription's
// sql_filter becomes the filter of its $Default rule, since what the
// module does with it cannot be seen.
const (
	adapterQueues = "asb_queues"
	adapterTopics = "asb_topics_name"
	adapterSubs   = "asb_subs"
)

// ModuleLists names the arguments of a module call that list entities.
// An empty name keeps the default (asb_queues, asb_topics_name, asb_subs).
type ModuleLists struct {
	Queues, Topics, Subscriptions string
}

// LoadModuleMap reads a JSON file that maps modules to the names of their
// entity list arguments, for modules whose source is not available:
//
//	{"bus": {"queues": "my_queues", "topics": "my_topics", "subscriptions": "my_subs"}, "*": {...}}
//
// A key is the label of the module block, its source, or "*" for the rest.
func LoadModuleMap(path string) (map[string]ModuleLists, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var m map[string]ModuleLists
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return m, nil
}

// listNames returns the list arguments of the module call blk.
func (b *builder) listNames(blk *hclsyntax.Block) map[string]string {
	names := map[string]string{adapterQueues: adapterQueues, adapterTopics: adapterTopics, adapterSubs: adapterSubs}
	source := ""
	if attr, ok := blk.Body.Attributes["source"]; ok {
		if v, diags := attr.Expr.Value(nil); !diags.HasErrors() && v.Type() == cty.String {
			source = v.AsString()
		}
	}
	for _, key := range []string{"*", source, blk.Labels[0]} {
		if l, ok := b.opts.ModuleMap[key]; ok && key != "" {
			for builtin, name := range map[string]string{adapterQueues: l.Queues, adapterTopics: l.Topics, adapterSubs: l.Subscriptions} {
				if name != "" {
					names[builtin] = name
				}
			}
		}
	}
	return names
}

// adaptModule adds the entities listed by a module call. It reports
// whether the call has any known list.
func adaptModule(b *builder, blk *hclsyntax.Block, ctx *hcl.EvalContext, address string) bool {
	found := false
	names := b.listNames(blk)
	for _, kind := range []string{adapterQueues, adapterTopics, adapterSubs} {
		list := names[kind]
		attr, ok := blk.Body.Attributes[list]
		if !ok {
			continue
		}
		found = true
		v, diags := attr.Expr.Value(ctx)
		if diags.HasErrors() || !v.IsWhollyKnown() {
			b.errorf("%s: %s is not known%s", address, list, detail(diags))
			continue
		}
		if v.IsNull() {
			continue
		}
		if !v.CanIterateElements() {
			b.errorf("%s: %s must be a list of objects", address, list)
			continue
		}
		i := 0
		for it := v.ElementIterator(); it.Next(); i++ {
			_, e := it.Element()
			adaptElement(b, kind, e, address+"."+list+"["+strconv.Itoa(i)+"]")
		}
	}
	return found
}

func adaptElement(b *builder, list string, e cty.Value, address string) {
	if e.IsNull() || !(e.Type().IsObjectType() || e.Type().IsMapType()) {
		b.errorf("%s: want an object", address)
		return
	}
	kind := map[string]broker.EntityKind{
		adapterQueues: broker.KindQueue,
		adapterTopics: broker.KindTopic,
		adapterSubs:   broker.KindSubscription,
	}[list]
	p := broker.DefaultProperties(kind)
	var name, topic, sqlFilter string
	ok := true
	for it := e.ElementIterator(); it.Next(); {
		k, v := it.Element()
		key := k.AsString()
		if v.IsNull() {
			continue
		}
		var err error
		switch {
		case key == "name":
			name, err = asString(v)
		case key == "topic" && kind == broker.KindSubscription:
			topic, err = asString(v)
		case key == "sql_filter" && kind == broker.KindSubscription:
			sqlFilter, err = asString(v)
		case properties[key] != nil:
			err = properties[key](&p, v)
		default:
			b.warnf("%s: %s is ignored", address, key)
		}
		if err != nil {
			b.errorf("%s: %s: %v", address, key, err)
			ok = false
		}
	}
	if name == "" {
		b.errorf("%s: name is required", address)
		return
	}
	if err := p.Validate(kind); err != nil {
		b.errorf("%s: %v", address, err)
		ok = false
	}
	if !ok {
		return
	}
	switch kind {
	case broker.KindQueue:
		b.addQueue(address, name, p)
	case broker.KindTopic:
		b.addTopic(address, name, p)
	case broker.KindSubscription:
		if topic == "" {
			b.errorf("%s: topic is required", address)
			return
		}
		s := b.addSubscription(address, topic, name, p)
		if s == nil || strings.TrimSpace(sqlFilter) == "" {
			return
		}
		r := broker.Rule{Name: broker.DefaultRuleName, Filter: broker.RuleFilter{Kind: broker.FilterSQL, SQL: sqlFilter}}
		if err := config.ValidateRule(r); err != nil {
			b.errorf("%s: sql_filter: %v", address, err)
			return
		}
		s.rules, s.filtered = []broker.Rule{r}, true
	}
}
