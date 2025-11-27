package events

type MBPDelta struct {
	Side  Side
	Price float64
	Size  float64
	Seq   uint64
}
