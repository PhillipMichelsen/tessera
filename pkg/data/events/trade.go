package events

type Trade struct {
	Price     float64
	Qty       float64
	Aggressor AggressorSide
	TradeID   string
}

type AggressorSide uint8

const (
	AggUnknown AggressorSide = iota
	AggBuy
	AggSell
)
