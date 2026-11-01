package domain

import "fmt"

// Money is an amount in minor units (cents). Integer arithmetic only:
// floating point is never used for money.
type Money int64

// String formats Money as "12.34".
func (m Money) String() string {
	sign := ""
	if m < 0 {
		sign = "-"
		m = -m
	}
	return fmt.Sprintf("%s%d.%02d", sign, m/100, m%100)
}

// Odds are decimal odds multiplied by 1000 (1.850 -> 1850).
// Storing odds as an integer keeps payout calculation exact and deterministic.
type Odds int64

// OddsScale is the fixed-point scale of Odds.
const OddsScale = 1000

// Payout returns stake * odds, rounded down to whole cents.
func (o Odds) Payout(stake Money) Money {
	return Money(int64(stake) * int64(o) / OddsScale)
}
