package internal

import (
	"context"
	"encoding/json"
	"log"

	nats "github.com/nats-io/nats.go"
)

type Symbol struct {
	SymbolId      string `json:"symbolId"`
	ProviderId    string `json:"providerId"`
	BaseAsset     string `json:"baseAsset"`
	QuoteCurrency string `json:"quoteCurrency"`
}

type SymbolRepository struct {
	logger *log.Logger
	js     nats.JetStreamContext
	bucket string
}

func NewSymbolRepository(logger *log.Logger, js nats.JetStreamContext, bucket string) *SymbolRepository {
	return &SymbolRepository{
		logger: logger,
		js:     js,
		bucket: bucket,
	}
}

func (r *SymbolRepository) Listen(ctx context.Context) (chan Symbol, chan string) {
	updatedCh := make(chan Symbol)
	deletedCh := make(chan string)
	go func() {
		r.watchSymbols(ctx, updatedCh, deletedCh)
	}()
	return updatedCh, deletedCh
}

func (r *SymbolRepository) watchSymbols(ctx context.Context, updatedCh chan Symbol, deletedCh chan string) {
	kvSymbols, err := r.js.KeyValue(r.bucket)
	if err != nil {
		r.logger.Fatal("Failed to load key-value store symbols:", err)
	}

	watcher, err := kvSymbols.Watch("*")
	if err != nil {
		r.logger.Fatal("Failed to watch symbols:", err)
	}

	go func() {
		<-ctx.Done()
		watcher.Stop()
	}()

	defer close(updatedCh)
	defer close(deletedCh)

	for update := range watcher.Updates() {
		if update == nil {
			continue
		}

		switch update.Operation() {
		case nats.KeyValuePut:
			r.logger.Println("Symbol updated:", update.Key())
			symbol := Symbol{}
			err = json.Unmarshal(update.Value(), &symbol)
			if err != nil {
				r.logger.Println("Failed to unmarshal symbol:", update.Key(), err)
				continue
			}
			if symbol.SymbolId != update.Key() {
				r.logger.Println("Key mismatch:", update.Key(), symbol.SymbolId)
				continue
			}
			updatedCh <- symbol

		case nats.KeyValueDelete:
			r.logger.Println("Symbol deleted:", update.Key())
			deletedCh <- update.Key()

		case nats.KeyValuePurge:
			r.logger.Println("Symbol purged:", update.Key())

		default:
			r.logger.Println("Unknown operation on symbol:", update.Operation())
		}
	}
}
