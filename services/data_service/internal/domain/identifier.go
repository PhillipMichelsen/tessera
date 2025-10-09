package domain

import (
	"errors"
	"sort"
	"strings"
)

var ErrBadIdentifier = errors.New("identifier: invalid format")

// Identifier is an immutable canonical key.
// Canonical form: "namespace::tag1[] . tag2[k=v;foo=bar] . tag3[]"
type Identifier struct{ key string }

// NewIdentifier builds a canonical key with strict validation.
// Tags and param keys are sorted. Tags with no params emit "name[]".
// Rejects on: empty namespace, bad tag names, bad keys/values.
func NewIdentifier(namespace string, tags map[string]map[string]string) (Identifier, error) {
	ns := strings.TrimSpace(namespace)
	if !validNamespace(ns) {
		return Identifier{}, ErrBadIdentifier
	}

	// Validate and copy to protect immutability and reject dup keys early.
	clean := make(map[string]map[string]string, len(tags))
	for name, params := range tags {
		n := strings.TrimSpace(name)
		if !validIDTagName(n) {
			return Identifier{}, ErrBadIdentifier
		}
		if _, exists := clean[n]; exists {
			// impossible via map input, but keep the intent explicit
			return Identifier{}, ErrBadIdentifier
		}
		if len(params) == 0 {
			clean[n] = map[string]string{}
			continue
		}
		dst := make(map[string]string, len(params))
		for k, v := range params {
			kk := strings.TrimSpace(k)
			vv := strings.TrimSpace(v)
			if !validParamKey(kk) || !validIDParamValue(vv) {
				return Identifier{}, ErrBadIdentifier
			}
			if _, dup := dst[kk]; dup {
				return Identifier{}, ErrBadIdentifier
			}
			dst[kk] = vv
		}
		clean[n] = dst
	}

	var b strings.Builder
	// Rough capacity hint.
	b.Grow(len(ns) + 2 + 16*len(clean) + 32)

	// namespace
	b.WriteString(ns)
	b.WriteString("::")

	// stable tag order
	names := make([]string, 0, len(clean))
	for n := range clean {
		names = append(names, n)
	}
	sort.Strings(names)

	for i, name := range names {
		if i > 0 {
			b.WriteByte('.')
		}
		b.WriteString(name)

		params := clean[name]
		if len(params) == 0 {
			b.WriteString("[]")
			continue
		}

		// stable param order
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

	return Identifier{key: b.String()}, nil
}

// NewIdentifierFromRaw wraps a raw key without validation.
func NewIdentifierFromRaw(raw string) Identifier { return Identifier{key: raw} }

// Key returns the canonical key string.
func (id Identifier) Key() string { return id.key }

// Parse returns namespace and tags from Key.
// Accepts "tag" (bare) as "tag[]". Emits "name[]"/"[k=v;...]". First token wins on duplicates.
func (id Identifier) Parse() (string, map[string]map[string]string, error) {
	k := id.key

	// namespace
	i := strings.Index(k, "::")
	if i <= 0 {
		return "", nil, ErrBadIdentifier
	}
	ns := strings.TrimSpace(k[:i])
	if !validNamespace(ns) {
		return "", nil, ErrBadIdentifier
	}
	raw := k[i+2:]

	tags := make(map[string]map[string]string, 8)
	if raw == "" {
		return ns, tags, nil
	}

	for tok := range strings.SplitSeq(raw, ".") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}

		lb := strings.IndexByte(tok, '[')
		if lb == -1 {
			// bare tag => empty params
			name := strings.TrimSpace(tok)
			if !validIDTagName(name) {
				return "", nil, ErrBadIdentifier
			}
			if _, exists := tags[name]; !exists {
				tags[name] = map[string]string{}
			}
			continue
		}

		rb := strings.LastIndexByte(tok, ']')
		if rb == -1 || rb < lb || rb != len(tok)-1 {
			return "", nil, ErrBadIdentifier
		}

		name := strings.TrimSpace(tok[:lb])
		if !validIDTagName(name) {
			return "", nil, ErrBadIdentifier
		}
		// first tag wins
		if _, exists := tags[name]; exists {
			continue
		}

		body := tok[lb+1 : rb]
		// forbid outer whitespace like "[ x=1 ]"
		if body != strings.TrimSpace(body) {
			return "", nil, ErrBadIdentifier
		}
		if body == "" {
			tags[name] = map[string]string{}
			continue
		}

		// parse "k=v;foo=bar"
		params := make(map[string]string, 4)
		for pair := range strings.SplitSeq(body, ";") {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) != 2 {
				return "", nil, ErrBadIdentifier
			}
			key := strings.TrimSpace(kv[0])
			val := strings.TrimSpace(kv[1])
			if !validParamKey(key) || !validIDParamValue(val) || val == "" {
				return "", nil, ErrBadIdentifier
			}
			// first key wins
			if _, dup := params[key]; !dup {
				params[key] = val
			}
		}
		tags[name] = params
	}

	return ns, tags, nil
}

// --- validation helpers ---

func validNamespace(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch r {
		case '[', ']', ':':
			return false
		}
		if isSpace(r) {
			return false
		}
	}
	return true
}

func validIDTagName(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch r {
		case '[', ']', '.', ':': // added ':'
			return false
		}
		if isSpace(r) {
			return false
		}
	}
	return true
}

func validParamKey(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch r {
		case '[', ']', ';', '=':
			return false
		}
		if isSpace(r) {
			return false
		}
	}
	return true
}

func validIDParamValue(s string) bool {
	// allow spaces; forbid only bracket, pair, and kv delimiters
	for _, r := range s {
		switch r {
		case '[', ']', ';', '=':
			return false
		}
	}
	return true
}

func isSpace(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' }
