package events

type MBODelta struct {
	Operation MBOOrderOp
	OrderID   string
	Side      Side
	Price     float64
	Size      float64
	IsMaker   bool
	Seq       uint64
	ParentID  string
}

type MBOOrderOp uint8

const (
	OrderAdd MBOOrderOp = iota
	OrderMod
	OrderDel
)
