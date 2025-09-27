// Package domain defines external message identifiers.
package domain

import (
	"errors"
	"sort"
	"strings"
)

var ErrBadIdentifier = errors.New("identifier: invalid format")

// Identifier is a lightweight wrapper around the canonical key.
type Identifier struct{ key string }

// NewIdentifier builds a canonical key: "namespace::l1.l2[param=v;...] .l3".
// Labels and params are sorted for determinism.
func NewIdentifier(namespace string, labels map[string]map[string]string) Identifier {
	var b strings.Builder
	// rough prealloc: ns + "::" + avg label + some params
	b.Grow(len(namespace) + 2 + 10*len(labels) + 20)

	// namespace
	b.WriteString(namespace)
	b.WriteString("::")

	// sort label names for stable output
	labelNames := make([]string, 0, len(labels))
	for name := range labels {
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)

	for i, name := range labelNames {
		if i > 0 {
			b.WriteByte('.')
		}
		b.WriteString(name)

		// params (sorted)
		params := labels[name]
		if len(params) > 0 {
			keys := make([]string, 0, len(params))
			for k := range params {
				keys = append(keys, k)
			}
			sort.Strings(keys)

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

	return Identifier{key: b.String()}
}

// NewIdentifierFromRaw wraps a raw key without validation.
func NewIdentifierFromRaw(raw string) Identifier { return Identifier{key: raw} }

// Key returns the canonical key string.
func (id Identifier) Key() string { return id.key }

// Parse returns namespace and labels from Key.
// Format: "namespace::label1.label2[param=a;foo=bar].label3"
func (id Identifier) Parse() (string, map[string]map[string]string, error) {
	k := id.key
	i := strings.Index(k, "::")
	if i <= 0 {
		return "", nil, ErrBadIdentifier
	}
	ns := k[:i]
	if ns == "" {
		return "", nil, ErrBadIdentifier
	}
	raw := k[i+2:]

	lbls := make(map[string]map[string]string, 8)
	if raw == "" {
		return ns, lbls, nil
	}

	for tok := range strings.SplitSeq(raw, ".") {
		if tok == "" {
			continue
		}
		name, params, err := parseLabel(tok)
		if err != nil || name == "" {
			return "", nil, ErrBadIdentifier
		}
		lbls[name] = params
	}
	return ns, lbls, nil
}

// parseLabel parses "name" or "name[k=v;...]" into (name, params).
func parseLabel(tok string) (string, map[string]string, error) {
	lb := strings.IndexByte(tok, '[')
	if lb == -1 {
		return tok, map[string]string{}, nil
	}
	rb := strings.LastIndexByte(tok, ']')
	if rb == -1 || rb < lb {
		return "", nil, ErrBadIdentifier
	}

	name := tok[:lb]
	paramStr := tok[lb+1 : rb]
	params := map[string]string{}
	if paramStr == "" {
		return name, params, nil
	}

	for pair := range strings.SplitSeq(paramStr, ";") {
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 || kv[0] == "" {
			return "", nil, ErrBadIdentifier
		}
		params[kv[0]] = kv[1]
	}
	return name, params, nil
}
