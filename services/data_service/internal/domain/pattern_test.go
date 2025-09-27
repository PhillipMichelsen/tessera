package domain

import (
	"reflect"
	"testing"
)

func TestNewPattern_Canonical_And_Superset(t *testing.T) {
	t.Run("canonical ordering and kinds", func(t *testing.T) {
		p, err := NewPattern("ns", map[string]TagSpec{
			"b": {Kind: MatchExact, Params: map[string]string{"y": "2", "x": "1"}},
			"a": {Kind: MatchNone},
			"c": {Kind: MatchAny},
		}, false)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := p.Key(), "ns::a[]:b[x=1;y=2]:c[*]"; got != want {
			t.Fatalf("got %q want %q", got, want)
		}
	})

	t.Run("superset via flag", func(t *testing.T) {
		p, err := NewPattern("ns", map[string]TagSpec{"a": {Kind: MatchNone}}, true)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := p.Key(), "ns::a[].*"; got != want {
			t.Fatalf("got %q want %q", got, want)
		}
	})

	t.Run("superset via '*' tag anywhere", func(t *testing.T) {
		p, err := NewPattern("ns", map[string]TagSpec{
			"*": {Kind: MatchAny}, // triggers superset; omitted from canonical
			"a": {Kind: MatchNone},
		}, false)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := p.Key(), "ns::a[].*"; got != want {
			t.Fatalf("got %q want %q", got, want)
		}
	})

	t.Run("trim and validate", func(t *testing.T) {
		p, err := NewPattern(" ns ", map[string]TagSpec{
			" tag ": {Kind: MatchAny},
		}, false)
		if err != nil {
			t.Fatal(err)
		}
		if p.Key() != "ns::tag[*]" {
			t.Fatalf("unexpected canonical: %q", p.Key())
		}
	})

	t.Run("reject invalid inputs", func(t *testing.T) {
		_, err := NewPattern("", nil, false)
		if err == nil {
			t.Fatal("expected error for empty namespace")
		}
		_, err = NewPattern("ns", map[string]TagSpec{"": {Kind: MatchAny}}, false)
		if err == nil {
			t.Fatal("expected error for empty tag")
		}
		_, err = NewPattern("ns", map[string]TagSpec{"bad:": {Kind: MatchAny}}, false)
		if err == nil {
			t.Fatal("expected error for ':' in tag")
		}
		_, err = NewPattern("ns", map[string]TagSpec{"a": {Kind: MatchExact, Params: map[string]string{"": "1"}}}, false)
		if err == nil {
			t.Fatal("expected error for empty param key")
		}
		_, err = NewPattern("ns", map[string]TagSpec{"a": {Kind: MatchExact, Params: map[string]string{"k": "bad;val"}}}, false)
		if err == nil {
			t.Fatal("expected error for bad param value")
		}
	})

	t.Run("MatchExact with empty params downgrades to []", func(t *testing.T) {
		// Behavior matches current constructor: empty exact => MatchNone
		p, err := NewPattern("ns", map[string]TagSpec{
			"a": {Kind: MatchExact, Params: map[string]string{}},
		}, false)
		if err != nil {
			t.Fatal(err)
		}
		if p.Key() != "ns::a[]" {
			t.Fatalf("unexpected canonical for empty exact: %q", p.Key())
		}
	})
}

func TestPattern_Parse_Tokens_And_SupersetRecognition(t *testing.T) {
	t.Run("accept :* token and .*", func(t *testing.T) {
		ns, specs, sup, err := NewPatternFromRaw("ns::a[]:*:b[*]").Parse()
		if err != nil {
			t.Fatal(err)
		}
		if ns != "ns" || !sup {
			t.Fatalf("ns=%q sup=%v", ns, sup)
		}
		if specs["a"].Kind != MatchNone || specs["b"].Kind != MatchAny {
			t.Fatalf("unexpected specs: %#v", specs)
		}

		_, specs2, sup2, err := NewPatternFromRaw("ns::a[]:b[*].*").Parse()
		if err != nil || !sup2 {
			t.Fatalf("parse superset suffix failed: err=%v sup=%v", err, sup2)
		}
		if !reflect.DeepEqual(specs, specs2) {
			t.Fatalf("specs mismatch between forms")
		}
	})

	t.Run("first-wins on duplicate tags and params", func(t *testing.T) {
		_, specs, sup, err := NewPatternFromRaw("ns::t[x=1;y=2]:t[*]:u[k=1;k=2]").Parse()
		if err != nil || sup {
			t.Fatalf("err=%v sup=%v", err, sup)
		}
		if specs["t"].Kind != MatchExact || specs["t"].Params["x"] != "1" {
			t.Fatalf("first tag should win: %#v", specs["t"])
		}
		if specs["u"].Params["k"] != "1" {
			t.Fatalf("first param key should win: %#v", specs["u"])
		}
	})

	t.Run("reject malformed", func(t *testing.T) {
		bads := []string{
			"", "no_ns", "ns:onecolon", "::missingns::tag[]",
			"ns::tag[", "ns::tag]", "ns::[]", "ns::tag[]junk",
			"ns::a[=1]", "ns::a[x=]", "ns::a[ x=1 ]",
		}
		for _, s := range bads {
			_, _, _, err := NewPatternFromRaw(s).Parse()
			if err == nil {
				t.Fatalf("expected parse error for %q", s)
			}
		}
	})
}

func TestPattern_Match_Matrix(t *testing.T) {
	makeID := func(key string) Identifier { return NewIdentifierFromRaw(key) }

	t.Run("namespace mismatch", func(t *testing.T) {
		p := NewPatternFromRaw("ns::a[]")
		if p.Match(makeID("other::a[]")) {
			t.Fatal("should not match different namespace")
		}
	})

	t.Run("MatchAny accepts empty and nonempty", func(t *testing.T) {
		p := NewPatternFromRaw("ns::a[*]")
		if !p.Match(makeID("ns::a[]")) || !p.Match(makeID("ns::a[x=1]")) {
			t.Fatal("MatchAny should accept both")
		}
	})

	t.Run("MatchNone requires empty", func(t *testing.T) {
		p := NewPatternFromRaw("ns::a[]")
		if !p.Match(makeID("ns::a[]")) {
			t.Fatal("empty should match")
		}
		if p.Match(makeID("ns::a[x=1]")) {
			t.Fatal("nonempty should not match MatchNone")
		}
	})

	t.Run("MatchExact equals, order independent", func(t *testing.T) {
		p := NewPatternFromRaw("ns::a[x=1;y=2]")
		if !p.Match(makeID("ns::a[y=2;x=1]")) {
			t.Fatal("param order should not matter")
		}
		if p.Match(makeID("ns::a[x=1]")) {
			t.Fatal("missing param should fail")
		}
		if p.Match(makeID("ns::a[x=1;y=2;z=3]")) {
			t.Fatal("extra param should fail exact")
		}
		if p.Match(makeID("ns::a[x=9;y=2]")) {
			t.Fatal("different value should fail")
		}
	})

	t.Run("exact-set vs superset", func(t *testing.T) {
		exact := NewPatternFromRaw("ns::a[]:b[*]")
		super := NewPatternFromRaw("ns::a[]:b[*].*")

		if !exact.Match(makeID("ns::a[].b[x=1]")) {
			t.Fatal("exact should match same set")
		}
		if exact.Match(makeID("ns::a[].b[x=1].c[]")) {
			t.Fatal("exact should not allow extra tags")
		}
		if !super.Match(makeID("ns::a[].b[x=1].c[]")) {
			t.Fatal("superset should allow extra tags")
		}
	})

	t.Run("all pattern tags must exist", func(t *testing.T) {
		p := NewPatternFromRaw("ns::a[]:b[*]")
		if p.Match(makeID("ns::a[]")) {
			t.Fatal("missing b should fail")
		}
	})
}
