package internal

import (
	"github.com/google/btree"
)

type PriceBook struct {
	btree *btree.BTree
}

type PriceBookEntry struct {
	Price  float64
	Amount float64
}

func (e *PriceBookEntry) Less(other btree.Item) bool {
	return e.Price < other.(*PriceBookEntry).Price
}

func NewPriceBook() *PriceBook {
	return &PriceBook{
		btree: btree.New(2),
	}
}

func (pb *PriceBook) RemoveByPrice(price float64) {
	pb.btree.Delete(&PriceBookEntry{Price: price})
}

func (pb *PriceBook) Upsert(price, amount float64) {
	pb.btree.ReplaceOrInsert(&PriceBookEntry{Price: price, Amount: amount})
}

func (pb *PriceBook) GetMin() float64 {
	if pb.btree.Len() == 0 {
		return 0
	}
	return pb.btree.Min().(*PriceBookEntry).Price
}

func (pb *PriceBook) GetMax() float64 {
	if pb.btree.Len() == 0 {
		return 0
	}
	return pb.btree.Max().(*PriceBookEntry).Price
}
