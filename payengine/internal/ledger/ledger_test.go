package ledger

import "testing"

func TestChargeEntriesAlwaysSumToZero(t *testing.T) {
	amounts := []int64{1, 2, 10, 29, 30, 31, 99, 100, 999, 1000, 10000, 123456, 99_999_999}
	for a := int64(1); a <= 3000; a++ {
		amounts = append(amounts, a)
	}
	for _, amount := range amounts {
		entries := ChargeEntries("m_test", amount)
		var sum int64
		for _, e := range entries {
			if e.Amount == 0 {
				t.Fatalf("amount %d: zero entry (the db would reject it)", amount)
			}
			sum += e.Amount
		}
		if sum != 0 {
			t.Fatalf("amount %d: entries sum to %d, want 0", amount, sum)
		}
	}
}

func TestFee(t *testing.T) {
	cases := []struct{ amount, want int64 }{
		{10000, 320}, // 290 + 30
		{1000, 59},   // 29 + 30
		{100, 32},    // 2 + 30
		{10, 10},     // fee can't be more than the payment
		{1, 1},
	}
	for _, c := range cases {
		if got := Fee(c.amount); got != c.want {
			t.Errorf("Fee(%d) = %d, want %d", c.amount, got, c.want)
		}
	}
}
