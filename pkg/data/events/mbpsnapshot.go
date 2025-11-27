package events

type MBPSnapshot struct {
	Bids  []PriceLevel
	Asks  []PriceLevel
	Depth int
	Seq   uint64
}

type PriceLevel struct {
	Price float64
	Size  float64
}
