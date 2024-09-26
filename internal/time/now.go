package time

import (
	"time"

	"github.com/tilinna/clock"
)

func NowMilli(clock clock.Clock) uint64 {
	return uint64((clock.Now().UnixNano() / int64(time.Millisecond)))
}
