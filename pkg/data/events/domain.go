// Package events ...
package events

type DataType uint8

const (
	TradeType DataType = iota
	QuoteType
	BarType
	MBPDeltaType
	MBPSnapshotType
	MBODeltaType
	MBOSnapshotType
	CustomType
)

type Side uint8

const (
	Bid Side = iota
	Ask
)
