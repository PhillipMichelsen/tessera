package manager

func workerEntryKey(w WorkerEntry) string {
	return w.Type + "|" + string(w.Spec) + "|" + string(w.Unit)
}

func workerEntryDiffs(old, nw []WorkerEntry) (added, removed []WorkerEntry) {
	oldKeys := make(map[string]struct{}, len(old))
	newKeys := make(map[string]struct{}, len(nw))

	for _, w := range old {
		oldKeys[workerEntryKey(w)] = struct{}{}
	}
	for _, w := range nw {
		k := workerEntryKey(w)
		newKeys[k] = struct{}{}
		if _, ok := oldKeys[k]; !ok {
			added = append(added, w)
		}
	}
	for _, w := range old {
		if _, ok := newKeys[workerEntryKey(w)]; !ok {
			removed = append(removed, w)
		}
	}
	return added, removed
}
