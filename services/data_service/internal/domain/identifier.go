package domain

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

const (
	prefixRaw      = "raw::"
	prefixInternal = "internal::"
)

// Identifier is a canonical representation of a data stream identifier.
type Identifier struct{ key string }

func (id Identifier) IsRaw() bool      { return strings.HasPrefix(id.key, prefixRaw) }
func (id Identifier) IsInternal() bool { return strings.HasPrefix(id.key, prefixInternal) }
func (id Identifier) Key() string      { return id.key }

func (id Identifier) ProviderSubject() (provider, subject string, ok bool) {
	if !id.IsRaw() {
		return "", "", false
	}
	body := strings.TrimPrefix(id.key, prefixRaw)
	prov, subj, ok := strings.Cut(body, ".")
	return prov, subj, ok
}

func (id Identifier) InternalParts() (venue, stream, symbol string, params map[string]string, ok bool) {
	if !id.IsInternal() {
		return "", "", "", nil, false
	}
	body := strings.TrimPrefix(id.key, prefixInternal)
	before, bracket, _ := strings.Cut(body, "[")
	parts := strings.Split(before, ".")
	if len(parts) != 3 {
		return "", "", "", nil, false
	}
	return parts[0], parts[1], parts[2], decodeParams(strings.TrimSuffix(bracket, "]")), true
}

func RawID(provider, subject string) (Identifier, error) {
	p := strings.ToLower(strings.TrimSpace(provider))
	s := strings.TrimSpace(subject)

	if err := validateComponent("provider", p, false); err != nil {
		return Identifier{}, err
	}
	if err := validateComponent("subject", s, true); err != nil {
		return Identifier{}, err
	}
	return Identifier{key: prefixRaw + p + "." + s}, nil
}

func InternalID(venue, stream, symbol string, params map[string]string) (Identifier, error) {
	v := strings.ToLower(strings.TrimSpace(venue))
	t := strings.ToLower(strings.TrimSpace(stream))
	sym := strings.ToUpper(strings.TrimSpace(symbol))

	if err := validateComponent("venue", v, false); err != nil {
		return Identifier{}, err
	}
	if err := validateComponent("stream", t, false); err != nil {
		return Identifier{}, err
	}
	if err := validateComponent("symbol", sym, false); err != nil {
		return Identifier{}, err
	}

	paramStr, err := encodeParams(params) // "k=v;..." or ""
	if err != nil {
		return Identifier{}, err
	}
	if paramStr == "" {
		paramStr = "[]"
	} else {
		paramStr = "[" + paramStr + "]"
	}
	return Identifier{key: prefixInternal + v + "." + t + "." + sym + paramStr}, nil
}

func ParseIdentifier(s string) (Identifier, error) {
	s = strings.TrimSpace(s)
	switch {
	case strings.HasPrefix(s, prefixRaw):
		// raw::provider.subject
		body := strings.TrimPrefix(s, prefixRaw)
		prov, subj, ok := strings.Cut(body, ".")
		if !ok {
			return Identifier{}, errors.New("invalid raw identifier: missing '.'")
		}
		return RawID(prov, subj)

	case strings.HasPrefix(s, prefixInternal):
		// internal::venue.stream.symbol[...]
		body := strings.TrimPrefix(s, prefixInternal)
		before, bracket, _ := strings.Cut(body, "[")
		parts := strings.Split(before, ".")
		if len(parts) != 3 {
			return Identifier{}, errors.New("invalid internal identifier: need venue.stream.symbol")
		}
		params := decodeParams(strings.TrimSuffix(bracket, "]"))
		return InternalID(parts[0], parts[1], parts[2], params)
	}
	return Identifier{}, errors.New("unknown identifier prefix")
}

var (
	segDisallow = regexp.MustCompile(`[ \t\r\n\[\]]`) // forbid whitespace/brackets in fixed segments
	dotDisallow = regexp.MustCompile(`[.]`)           // fixed segments cannot contain '.'
)

// allowAny=true (for subject) skips dot checks but still forbids whitespace/brackets.
func validateComponent(name, v string, allowAny bool) error {
	if v == "" {
		return fmt.Errorf("%s cannot be empty", name)
	}
	if allowAny {
		if segDisallow.MatchString(v) {
			return fmt.Errorf("%s contains illegal chars [] or whitespace", name)
		}
		return nil
	}
	if segDisallow.MatchString(v) || dotDisallow.MatchString(v) {
		return fmt.Errorf("%s contains illegal chars (dot/brackets/whitespace)", name)
	}
	return nil
}

// encodeParams renders sorted k=v pairs separated by ';'.
func encodeParams(params map[string]string) (string, error) {
	if len(params) == 0 {
		return "", nil
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		k = strings.ToLower(strings.TrimSpace(k))
		if k == "" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]string, 0, len(keys))
	for _, k := range keys {
		v := strings.TrimSpace(params[k])
		// prevent breaking delimiters
		if strings.ContainsAny(k, ";]") || strings.ContainsAny(v, ";]") {
			return "", fmt.Errorf("param %q contains illegal ';' or ']'", k)
		}
		out = append(out, k+"="+v)
	}
	return strings.Join(out, ";"), nil
}

func decodeParams(s string) map[string]string {
	s = strings.TrimSpace(s)
	if s == "" {
		return map[string]string{}
	}
	out := make(map[string]string, 4)
	for _, p := range strings.Split(s, ";") {
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			continue
		}
		k := strings.ToLower(strings.TrimSpace(kv[0]))
		v := strings.TrimSpace(kv[1])
		if k != "" {
			out[k] = v
		}
	}
	return out
}
