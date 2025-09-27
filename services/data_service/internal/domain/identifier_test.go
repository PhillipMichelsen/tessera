package domain

import (
	"reflect"
	"testing"
)

func TestNewIdentifier_CanonicalAndValidation(t *testing.T) {
	t.Run("canonical ordering and formatting", func(t *testing.T) {
		id, err := NewIdentifier("ns", map[string]map[string]string{
			"b": {"y": "2", "x": "1"},
			"a": {},
		})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		got := id.Key()
		want := "ns::a[].b[x=1;y=2]"
		if got != want {
			t.Fatalf("key mismatch\ngot:  %q\nwant: %q", got, want)
		}
	})

	t.Run("trim whitespace and validate", func(t *testing.T) {
		id, err := NewIdentifier(" ns ", map[string]map[string]string{
			" tag ": {" k ": " v "},
		})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if id.Key() != "ns::tag[k=v]" {
			t.Fatalf("unexpected canonical: %q", id.Key())
		}
	})

	t.Run("reject bad namespace", func(t *testing.T) {
		cases := []string{"", "a:b", "a[b]"}
		for _, ns := range cases {
			if _, err := NewIdentifier(ns, nil); err == nil {
				t.Fatalf("expected error for ns=%q", ns)
			}
		}
	})

	t.Run("reject bad tag names", func(t *testing.T) {
		for _, name := range []string{"", "bad.", "bad[", "bad]", "a:b"} {
			_, err := NewIdentifier("ns", map[string]map[string]string{
				name: {},
			})
			if err == nil {
				t.Fatalf("expected error for tag name %q", name)
			}
		}
	})

	t.Run("reject bad param keys and values", func(t *testing.T) {
		badKeys := []string{"", "k;", "k[", "k]", "k="}
		for _, k := range badKeys {
			if _, err := NewIdentifier("ns", map[string]map[string]string{
				"t": {k: "ok"},
			}); err == nil {
				t.Fatalf("expected error for bad key %q", k)
			}
		}
		for _, v := range []string{"bad;", "bad[", "bad]", "a=b"} {
			if _, err := NewIdentifier("ns", map[string]map[string]string{
				"t": {"k": v},
			}); err == nil {
				t.Fatalf("expected error for bad value %q", v)
			}
		}
	})
}

func TestIdentifier_Parse_RoundTripAndTolerance(t *testing.T) {
	t.Run("round trip from constructor", func(t *testing.T) {
		id, err := NewIdentifier("ns", map[string]map[string]string{
			"a": {},
			"b": {"x": "1", "y": "2"},
		})
		if err != nil {
			t.Fatal(err)
		}
		ns, tags, err := id.Parse()
		if err != nil {
			t.Fatal(err)
		}
		if ns != "ns" {
			t.Fatalf("ns: got %q", ns)
		}
		want := map[string]map[string]string{"a": {}, "b": {"x": "1", "y": "2"}}
		if !reflect.DeepEqual(tags, want) {
			t.Fatalf("tags mismatch\ngot:  %#v\nwant: %#v", tags, want)
		}
	})

	t.Run("parse bare tag as empty params", func(t *testing.T) {
		id := NewIdentifierFromRaw("ns::a.b[]")
		_, tags, err := id.Parse()
		if err != nil {
			t.Fatal(err)
		}
		if len(tags["a"]) != 0 || len(tags["b"]) != 0 {
			t.Fatalf("expected empty params, got %#v", tags)
		}
	})

	t.Run("first token wins on duplicate tags and params", func(t *testing.T) {
		id := NewIdentifierFromRaw("ns::t[x=1;y=2].t[x=9].u[k=1;k=2]")
		_, tags, err := id.Parse()
		if err != nil {
			t.Fatal(err)
		}
		if tags["t"]["x"] != "1" || tags["t"]["y"] != "2" {
			t.Fatalf("first tag should win, got %#v", tags["t"])
		}
		if tags["u"]["k"] != "1" {
			t.Fatalf("first param key should win, got %#v", tags["u"])
		}
	})

	t.Run("reject malformed", func(t *testing.T) {
		bads := []string{
			"", "no_ns", "ns:onecolon", "::missingns::tag[]", "ns::tag[", "ns::tag]", "ns::[]",
			"ns::tag[]junk", "ns::tag[x=1;y]", "ns::tag[=1]", "ns::tag[ x=1 ]", // spaces inside keys are rejected
		}
		for _, s := range bads {
			_, _, err := NewIdentifierFromRaw(s).Parse()
			if err == nil {
				t.Fatalf("expected parse error for %q", s)
			}
		}
	})
}

func TestIdentifier_NewThenParse_ForbidsColonInTagName(t *testing.T) {
	_, err := NewIdentifier("ns", map[string]map[string]string{"a:b": {}})
	if err == nil {
		t.Fatal("expected error due to ':' in tag name")
	}
}
