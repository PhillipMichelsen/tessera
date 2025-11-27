package events

import "time"

type Bar struct {
	Open     float64
	High     float64
	Low      float64
	Close    float64
	Volume   float64
	Interval time.Duration
}
