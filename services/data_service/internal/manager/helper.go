package manager

import (
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

func identifierSetDifferences(oldIDs, nextIDs []domain.Identifier) (toAdd, toDel []domain.Identifier) {
	oldSet := make(map[domain.Identifier]struct{}, len(oldIDs))
	for _, id := range oldIDs {
		oldSet[id] = struct{}{}
	}

	newSet := make(map[domain.Identifier]struct{}, len(nextIDs))
	for _, id := range nextIDs {
		newSet[id] = struct{}{}
		if _, ok := oldSet[id]; !ok {
			toAdd = append(toAdd, id)
		}
	}

	for _, id := range oldIDs {
		if _, ok := newSet[id]; !ok {
			toDel = append(toDel, id)
		}
	}

	return
}

func identifierMapToSlice(m map[domain.Identifier]struct{}) []domain.Identifier {
	ids := make([]domain.Identifier, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	return ids
}
