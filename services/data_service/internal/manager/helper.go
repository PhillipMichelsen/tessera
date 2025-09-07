package manager

import (
	"fmt"

	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/provider"
)

// Lightweight error helper to define package-level errors inline.
type constErr string

func (e constErr) Error() string { return string(e) }
func errorf(s string) error      { return constErr(s) }

// copySet copies a set of identifiers to a new map.
func copySet(in map[domain.Identifier]struct{}) map[domain.Identifier]struct{} {
	out := make(map[domain.Identifier]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}

// identifierSetDifferences computes additions and deletions from old -> next.
func identifierSetDifferences(old map[domain.Identifier]struct{}, next []domain.Identifier) (toAdd, toDel []domain.Identifier) {
	newSet := make(map[domain.Identifier]struct{}, len(next))
	for _, id := range next {
		newSet[id] = struct{}{}
		if _, ok := old[id]; !ok {
			toAdd = append(toAdd, id)
		}
	}
	for id := range old {
		if _, ok := newSet[id]; !ok {
			toDel = append(toDel, id)
		}
	}
	return
}

// joinErrors aggregates multiple errors.
type joined struct{ es []error }

func (j joined) Error() string {
	switch n := len(j.es); {
	case n == 0:
		return ""
	case n == 1:
		return j.es[0].Error()
	default:
		s := j.es[0].Error()
		for i := 1; i < n; i++ {
			s += "; " + j.es[i].Error()
		}
		return s
	}
}

func join(es []error) error {
	if len(es) == 0 {
		return nil
	}
	return joined{es}
}

// resolveProvider parses a raw identifier and looks up the provider.
func (m *Manager) resolveProvider(id domain.Identifier) (provider.Provider, string, error) {
	provName, subj, ok := id.ProviderSubject()
	if !ok || provName == "" || subj == "" {
		return nil, "", ErrInvalidIdentifier
	}
	p := m.providers[provName]
	if p == nil {
		return nil, "", fmt.Errorf("%w: %s", ErrUnknownProvider, provName)
	}
	return p, subj, nil
}

// incrementStreamRefCount increments refcount and returns true if transitioning 0->1.
func (m *Manager) incrementStreamRefCount(id domain.Identifier) bool {
	rc := m.streamRef[id] + 1
	m.streamRef[id] = rc
	return rc == 1
}

// decrementStreamRefCount decrements refcount and returns true if transitioning 1->0.
func (m *Manager) decrementStreamRefCount(id domain.Identifier) bool {
	rc, ok := m.streamRef[id]
	if !ok {
		return false
	}
	rc--
	if rc <= 0 {
		delete(m.streamRef, id)
		return true
	}
	m.streamRef[id] = rc
	return false
}
