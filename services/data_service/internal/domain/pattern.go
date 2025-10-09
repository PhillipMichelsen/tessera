package domain

import (
	"errors"
	"sort"
	"strings"
)

var ErrBadPattern = errors.New("pattern: invalid format")

// ParamMatchKind selects how a tag's params must match.
type ParamMatchKind uint8

const (
	MatchAny   ParamMatchKind = iota // "tag" or "tag[*]"
	MatchNone                        // "tag[]"
	MatchExact                       // "tag[k=v;...]"
)

// TagSpec is the per-tag constraint.
type TagSpec struct {
	Kind   ParamMatchKind
	Params map[string]string // only for MatchExact; keys sorted on emit
}

// Pattern is an immutable canonical key.
// Canonical form (tags unordered in input, sorted on emit):
//
//	namespace::tag1[]:tag2[*]:tag3[k=v;foo=bar].*        // superset
//	namespace::tag1[]:tag2[*]:tag3[k=v;foo=bar]          // exact set
type Pattern struct{ key string }

// NewPattern builds the canonical key from structured input with strict validation.
// If a tag name equals "*" it sets superset and omits it from canonical tags.
func NewPattern(namespace string, tags map[string]TagSpec, superset bool) (Pattern, error) {
	ns := strings.TrimSpace(namespace)
	if !validNamespace(ns) {
		return Pattern{}, ErrBadPattern
	}

	// Validate tags and normalize.
	clean := make(map[string]TagSpec, len(tags))
	for name, spec := range tags {
		n := strings.TrimSpace(name)
		if n == "*" {
			superset = true
			continue
		}
		if !validPatternTagName(n) {
			return Pattern{}, ErrBadPattern
		}
		switch spec.Kind {
		case MatchAny:
			clean[n] = TagSpec{Kind: MatchAny}
		case MatchNone:
			clean[n] = TagSpec{Kind: MatchNone}
		case MatchExact:
			if len(spec.Params) == 0 {
				// Treat empty exact as none.
				clean[n] = TagSpec{Kind: MatchNone}
				continue
			}
			dst := make(map[string]string, len(spec.Params))
			for k, v := range spec.Params {
				kk := strings.TrimSpace(k)
				vv := strings.TrimSpace(v)
				if !validParamKey(kk) || !validPatternParamValue(vv) {
					return Pattern{}, ErrBadPattern
				}
				if _, dup := dst[kk]; dup {
					return Pattern{}, ErrBadPattern
				}
				dst[kk] = vv
			}
			clean[n] = TagSpec{Kind: MatchExact, Params: dst}
		default:
			// Reject unknown kinds rather than silently defaulting.
			return Pattern{}, ErrBadPattern
		}
	}

	var b strings.Builder
	b.Grow(len(ns) + 2 + 16*len(clean) + 32 + 2)

	b.WriteString(ns)
	b.WriteString("::")

	names := make([]string, 0, len(clean))
	for n := range clean {
		names = append(names, n)
	}
	sort.Strings(names)

	for i, name := range names {
		if i > 0 {
			b.WriteByte(':')
		}
		b.WriteString(name)

		spec := clean[name]
		switch spec.Kind {
		case MatchAny:
			b.WriteString("[*]")
		case MatchNone:
			b.WriteString("[]")
		case MatchExact:
			keys := make([]string, 0, len(spec.Params))
			for k := range spec.Params {
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
				b.WriteString(spec.Params[k])
			}
			b.WriteByte(']')
		}
	}

	if superset {
		if len(names) > 0 {
			b.WriteByte('.')
		}
		b.WriteByte('*')
	}
	return Pattern{key: b.String()}, nil
}

// NewPatternFromRaw wraps a raw key without validation.
func NewPatternFromRaw(raw string) Pattern { return Pattern{key: raw} }

// Key returns the canonical key string.
func (p Pattern) Key() string { return p.key }

// Parse returns namespace, tag specs, and superset flag.
// Accepts tokens: "tag", "tag[*]", "tag[]", "tag[k=v;...]". Also accepts ".*" suffix or a ":*" token anywhere.
// First token wins on duplicate tag names; first key wins on duplicate params.
func (p Pattern) Parse() (string, map[string]TagSpec, bool, error) {
	k := p.key

	// namespace
	i := strings.Index(k, "::")
	if i <= 0 {
		return "", nil, false, ErrBadPattern
	}
	ns := strings.TrimSpace(k[:i])
	if !validNamespace(ns) {
		return "", nil, false, ErrBadPattern
	}
	raw := k[i+2:]

	// suffix superset ".*"
	superset := false
	if strings.HasSuffix(raw, ".*") {
		superset = true
		raw = raw[:len(raw)-2]
	}

	specs := make(map[string]TagSpec, 8)
	if raw == "" {
		return ns, specs, superset, nil
	}

	for tok := range strings.SplitSeq(raw, ":") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		if tok == "*" {
			superset = true
			continue
		}

		lb := strings.IndexByte(tok, '[')
		if lb == -1 {
			name := tok
			if !validPatternTagName(name) {
				return "", nil, false, ErrBadPattern
			}
			if _, exists := specs[name]; !exists {
				specs[name] = TagSpec{Kind: MatchAny}
			}
			continue
		}

		rb := strings.LastIndexByte(tok, ']')
		if rb == -1 || rb < lb || rb != len(tok)-1 {
			return "", nil, false, ErrBadPattern
		}

		name := strings.TrimSpace(tok[:lb])
		if !validPatternTagName(name) {
			return "", nil, false, ErrBadPattern
		}
		// first tag wins
		if _, exists := specs[name]; exists {
			continue
		}

		rawBody := tok[lb+1 : rb]
		// forbid outer whitespace like "[ x=1 ]"
		if rawBody != strings.TrimSpace(rawBody) {
			return "", nil, false, ErrBadPattern
		}
		body := strings.TrimSpace(rawBody)

		switch body {
		case "":
			specs[name] = TagSpec{Kind: MatchNone}
		case "*":
			specs[name] = TagSpec{Kind: MatchAny}
		default:
			params := make(map[string]string, 4)
			for pair := range strings.SplitSeq(body, ";") {
				pair = strings.TrimSpace(pair)
				if pair == "" {
					continue
				}
				kv := strings.SplitN(pair, "=", 2)
				if len(kv) != 2 {
					return "", nil, false, ErrBadPattern
				}
				key := strings.TrimSpace(kv[0])
				val := strings.TrimSpace(kv[1])
				if !validParamKey(key) || !validPatternParamValue(val) || val == "" {
					return "", nil, false, ErrBadPattern
				}
				// first key wins
				if _, dup := params[key]; !dup {
					params[key] = val
				}
			}
			specs[name] = TagSpec{Kind: MatchExact, Params: params}
		}
	}

	return ns, specs, superset, nil
}

// Equal compares canonical keys.
func (p Pattern) Equal(q Pattern) bool { return p.key == q.key }

// CompiledPattern is a parsed pattern optimized for matching.
type CompiledPattern struct {
	ns       string
	superset bool
	specs    map[string]TagSpec
}

// Compile parses and returns a compiled form.
func (p Pattern) Compile() (CompiledPattern, error) {
	ns, specs, sup, err := p.Parse()
	if err != nil {
		return CompiledPattern{}, err
	}
	return CompiledPattern{ns: ns, specs: specs, superset: sup}, nil
}

// Parse on CompiledPattern returns the structured contents without error.
func (cp CompiledPattern) Parse() (namespace string, tags map[string]TagSpec, superset bool) {
	return cp.ns, cp.specs, cp.superset
}

// Match parses id and tests it against the pattern.
// Returns false on parse error.
func (p Pattern) Match(id Identifier) bool {
	cp, err := p.Compile()
	if err != nil {
		return false
	}
	return cp.Match(id)
}

// Match tests id against the compiled pattern.
func (cp CompiledPattern) Match(id Identifier) bool {
	ns, tags, err := id.Parse()
	if err != nil || ns != cp.ns {
		return false
	}

	// All pattern tags must be satisfied.
	for name, spec := range cp.specs {
		params, ok := tags[name]
		if !ok {
			return false
		}
		switch spec.Kind {
		case MatchAny:
			// any or none is fine
		case MatchNone:
			if len(params) != 0 {
				return false
			}
		case MatchExact:
			if len(params) != len(spec.Params) {
				return false
			}
			for k, v := range spec.Params {
				if params[k] != v {
					return false
				}
			}
		default:
			return false
		}
	}

	// If exact-set match, forbid extra tags.
	if !cp.superset && len(tags) != len(cp.specs) {
		return false
	}
	return true
}

// --- validation helpers ---

func validPatternTagName(s string) bool {
	if s == "" || s == "*" {
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

func validPatternParamValue(s string) bool {
	// allow spaces; forbid only bracket, pair, and kv delimiters
	for _, r := range s {
		switch r {
		case '[', ']', ';', '=':
			return false
		}
	}
	return true
}
