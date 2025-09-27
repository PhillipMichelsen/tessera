package domain

import (
	"sort"
	"strings"
)

type Pattern struct {
	Namespace string
	Labels    map[string]map[string]string
	Exact     bool
}

// Canonical returns a canonical string representation of the Pattern struct
// TODO: Ensure labels and namespaces are set to lowercase
func (p *Pattern) Canonical() string {
	var b strings.Builder
	b.Grow(len(p.Namespace) + 10*len(p.Labels) + 20) // preallocate a rough size estimate

	b.WriteString(p.Namespace)
	b.WriteString("::")

	labelNames := make([]string, 0, len(p.Labels))
	for name := range p.Labels {
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames) // sort the labels for determinism

	for i, name := range labelNames {
		if i > 0 {
			b.WriteByte('|')
		}
		b.WriteString(name)

		params := p.Labels[name]
		if len(params) > 0 {
			keys := make([]string, 0, len(params))
			for k := range params {
				keys = append(keys, k)
			}
			sort.Strings(keys) // sort params for determinism

			b.WriteByte('[')
			for j, k := range keys {
				if j > 0 {
					b.WriteByte(';')
				}
				b.WriteString(k)
				b.WriteByte('=')
				b.WriteString(params[k])
			}
			b.WriteByte(']')
		}
	}

	b.WriteString("::")
	if p.Exact {
		b.WriteString("t")
	} else {
		b.WriteString("f")
	}

	return b.String()
}

// Satisfies checks if a domain.Identifier satisfies the pattern.
func (p *Pattern) Satisfies(id Identifier) bool {
	ns, idLabels, err := id.Parse()
	if err != nil || ns != p.Namespace {
		return false
	}

	// Every pattern label must be present in the identifier.
	for lname, wantParams := range p.Labels {
		haveParams, ok := idLabels[lname]
		if !ok {
			return false
		}
		// If pattern specifies params, they must be a subset of identifier's params.
		for k, v := range wantParams {
			hv, ok := haveParams[k]
			if !ok || hv != v {
				return false
			}
		}
		// If pattern has no params for this label, it matches any/none params in the identifier.
	}

	// Exact applies to label names only: no extras allowed.
	if p.Exact && len(idLabels) != len(p.Labels) {
		return false
	}
	return true
}
