package events

type MBOSnapshot struct {
	Orders []OrderEntry
	Seq    uint64
}

type OrderEntry struct {
	OrderID string
	Side    Side
	Price   float64
	Size    float64
	IsMaker bool
}
