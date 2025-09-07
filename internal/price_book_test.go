package internal

import (
	"testing"
)

func TestPriceBook_Empty(t *testing.T) {
	pb := NewPriceBook()

	if got := pb.GetMin(); got != 0 {
		t.Fatalf("expected min 0 for empty book, got %v", got)
	}
	if got := pb.GetMax(); got != 0 {
		t.Fatalf("expected max 0 for empty book, got %v", got)
	}
}

func TestPriceBook_UpsertAndOrdering(t *testing.T) {
	pb := NewPriceBook()

	pb.Upsert(100, 1.5)
	if pb.GetMin() != 100 || pb.GetMax() != 100 {
		t.Fatalf("expected min=max=100 after first insert, got min=%v max=%v", pb.GetMin(), pb.GetMax())
	}

	pb.Upsert(90, 2.0)
	if pb.GetMin() != 90 {
		t.Fatalf("expected min=90 after inserting lower price, got %v", pb.GetMin())
	}
	if pb.GetMax() != 100 {
		t.Fatalf("expected max=100 unchanged, got %v", pb.GetMax())
	}

	pb.Upsert(110, 3.0)
	if pb.GetMax() != 110 {
		t.Fatalf("expected max=110 after inserting higher price, got %v", pb.GetMax())
	}
	if pb.GetMin() != 90 {
		t.Fatalf("expected min=90 unchanged, got %v", pb.GetMin())
	}
}

func TestPriceBook_UpsertReplaceSamePrice(t *testing.T) {
	pb := NewPriceBook()

	pb.Upsert(100, 1.0)
	if pb.GetMin() != 100 || pb.GetMax() != 100 {
		t.Fatalf("expected min=max=100 after insert, got min=%v max=%v", pb.GetMin(), pb.GetMax())
	}

	// Replace amount at the same price; min/max should remain the same
	pb.Upsert(100, 2.5)
	if pb.GetMin() != 100 || pb.GetMax() != 100 {
		t.Fatalf("expected min=max=100 after replace, got min=%v max=%v", pb.GetMin(), pb.GetMax())
	}
}

func TestPriceBook_RemoveByPrice(t *testing.T) {
	pb := NewPriceBook()

	pb.Upsert(90, 1)
	pb.Upsert(100, 1)
	pb.Upsert(110, 1)

	if pb.GetMin() != 90 || pb.GetMax() != 110 {
		t.Fatalf("precondition failed: expected min=90 max=110, got min=%v max=%v", pb.GetMin(), pb.GetMax())
	}

	// Remove current min
	pb.RemoveByPrice(90)
	if pb.GetMin() != 100 {
		t.Fatalf("expected min=100 after removing 90, got %v", pb.GetMin())
	}
	if pb.GetMax() != 110 {
		t.Fatalf("expected max=110 unchanged after removing 90, got %v", pb.GetMax())
	}

	// Remove current max
	pb.RemoveByPrice(110)
	if pb.GetMax() != 100 {
		t.Fatalf("expected max=100 after removing 110, got %v", pb.GetMax())
	}
	if pb.GetMin() != 100 {
		t.Fatalf("expected min=100 unchanged after removing 110, got %v", pb.GetMin())
	}

	// Remove the last remaining price
	pb.RemoveByPrice(100)
	if pb.GetMin() != 0 || pb.GetMax() != 0 {
		t.Fatalf("expected empty book after removing last price, got min=%v max=%v", pb.GetMin(), pb.GetMax())
	}
}

func TestPriceBook_RemoveNonExistent_NoPanicAndNoChange(t *testing.T) {
	pb := NewPriceBook()
	pb.Upsert(100, 1)

	minBefore, maxBefore := pb.GetMin(), pb.GetMax()

	// Remove a price that does not exist
	pb.RemoveByPrice(123.45)

	if pb.GetMin() != minBefore || pb.GetMax() != maxBefore {
		t.Fatalf("expected no change after removing non-existent price, got min=%v max=%v", pb.GetMin(), pb.GetMax())
	}
}
