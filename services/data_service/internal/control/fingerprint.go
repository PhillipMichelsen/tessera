package control

import (
	"encoding/binary"
	"encoding/hex"
	"sort"

	"lukechampine.com/blake3"
)

func StreamFingerprint(templateFP, outPort string, inMap map[string]string) string {
	// Sort input keys for determinism.
	keys := make([]string, 0, len(inMap))
	for k := range inMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := blake3.New(32, nil)

	write := func(s string) {
		var lenbuf [4]byte
		binary.LittleEndian.PutUint32(lenbuf[:], uint32(len(s)))
		_, _ = h.Write(lenbuf[:])
		_, _ = h.Write([]byte(s))
	}

	// templateFP, outPort, input count, then pairs.
	write(templateFP)
	write(outPort)

	var nbuf [4]byte
	binary.LittleEndian.PutUint32(nbuf[:], uint32(len(keys)))
	_, _ = h.Write(nbuf[:])

	for _, k := range keys {
		write(k)
		write(inMap[k])
	}

	sum := h.Sum(nil)
	return hex.EncodeToString(sum)
}
