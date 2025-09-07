package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sync"
	"time"

	uuid "github.com/google/uuid"
	nats "github.com/nats-io/nats.go"
	internal "github.com/shatilov-diman/rates.derby-connectors/internal"
	types "github.com/shatilov-diman/rates.derby-ticks_feeder/pkg/types"
)

type TickerProcessor struct {
	logger                 *log.Logger
	symbolsProviderFactory internal.SymbolsProviderFunc

	symbolsMutex sync.Mutex
	symbols      map[string]internal.Symbol
	revSymbols   map[string]internal.Symbol

	bids map[string]*internal.PriceBook
	asks map[string]*internal.PriceBook
	last map[string]types.Tick

	ws       *internal.WS
	wg       sync.WaitGroup
	commands chan internal.WSCommand
	channels map[float64]string
	done     chan struct{}
	tickCh   chan types.Tick
}

func NewTickerProcessor(logger *log.Logger, provider internal.SymbolsProviderFunc) *TickerProcessor {
	commands := make(chan internal.WSCommand, 100)
	return &TickerProcessor{
		logger:                 logger,
		symbolsProviderFactory: provider,
		symbolsMutex:           sync.Mutex{},
		symbols:                make(map[string]internal.Symbol),
		revSymbols:             make(map[string]internal.Symbol),
		bids:                   make(map[string]*internal.PriceBook),
		asks:                   make(map[string]*internal.PriceBook),
		last:                   make(map[string]types.Tick),
		commands:               commands,
		ws:                     internal.NewWS(commands),
		wg:                     sync.WaitGroup{},
		channels:               make(map[float64]string),
		done:                   make(chan struct{}),
		tickCh:                 make(chan types.Tick, 1000),
	}
}

func (p *TickerProcessor) onUpdateSymbol(symbol internal.Symbol) {
	p.symbolsMutex.Lock()
	defer p.symbolsMutex.Unlock()

	p.symbols[symbol.SymbolId] = symbol
	p.revSymbols[symbol.ProviderId] = symbol

	cmd := map[string]any{
		"event":   "subscribe",
		"channel": "book",
		"symbol":  symbol.ProviderId,
		"prec":    "P0",
		"len":     "25",
	}
	p.logger.Println("Sending subscribe command:", cmd)
	p.commands <- cmd
}

func (p *TickerProcessor) onDeleteSymbol(symbolId string) {
	p.symbolsMutex.Lock()
	defer p.symbolsMutex.Unlock()

	symbol, ok := p.symbols[symbolId]
	if !ok {
		return
	}

	delete(p.symbols, symbolId)
	delete(p.revSymbols, symbol.ProviderId)

	_ = symbol
	// Bitfinex doesn't support unsubscribe
	// cmd := map[string]any{
	// 	"event":   "unsubscribe",
	// 	"channel": "book",
	// 	"symbol":  symbol.ProviderId,
	// }
	// p.logger.Println("Sending unsubscribe command:", cmd)
	// p.commands <- cmd
}

func (p *TickerProcessor) getSymbolByProviderId(providerId string) *internal.Symbol {
	p.symbolsMutex.Lock()
	defer p.symbolsMutex.Unlock()

	symbol, ok := p.revSymbols[providerId]
	if !ok {
		return nil
	}

	return &symbol
}

func (p *TickerProcessor) Run(ctx context.Context) {

	go func() {
		p.run(ctx)
	}()
}

func (p *TickerProcessor) Wait(ctx context.Context) {
	<-p.done
}

func (p *TickerProcessor) Ticks() <-chan types.Tick {
	return p.tickCh
}

func (p *TickerProcessor) run(ctx context.Context) {
	messages, connect, err := p.ws.Connect(ctx, "wss://api.bitfinex.com/ws/2")
	if err != nil {
		p.logger.Println("Error connecting to Bitfinex: ", err)
		return
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		for range connect {
			p.logger.Println("Connected to Bitfinex")
		}
	}()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		for msg := range messages {
			p.process(ctx, msg)
		}
	}()

	p.wg.Wait()

	close(p.done)
	close(p.commands)
}

func (p *TickerProcessor) OnHello(ctx context.Context) {
	symbolsProvider := p.symbolsProviderFactory()
	updatedCh, deletedCh := symbolsProvider.Listen(ctx)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		for symbol := range updatedCh {
			p.onUpdateSymbol(symbol)
		}
	}()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		for symbolId := range deletedCh {
			p.onDeleteSymbol(symbolId)
		}
	}()
}

func (p *TickerProcessor) process(ctx context.Context, msg internal.WSMessage) {
	switch msg := msg.(type) {
	case map[string]any:
		switch msg["event"] {
		case "info":
			p.logger.Println("Info message:", msg)
			p.OnHello(ctx)
		case "error":
			message, _ := msg["msg"].(string)
			pair, _ := msg["pair"].(string)
			p.logger.Println("Error message:", message, pair)
			p.logger.Println("Error message:", msg)
		case "subscribed":
			chanId, _ := msg["chanId"].(float64)
			pair, _ := msg["pair"].(string)
			p.channels[chanId] = pair
			p.logger.Println("Subscribed to:", pair, "on channel:", chanId)
		default:
			p.logger.Println("Unknown message:", msg)
		}

	case []any:
		if len(msg) == 2 {
			chanId := msg[0].(float64)
			symbol := p.getSymbolByProviderId(p.channels[chanId])
			if symbol == nil {
				return
			}
			pair := symbol.SymbolId

			bids, ok := p.bids[pair]
			if !ok {
				bids = internal.NewPriceBook()
				p.bids[pair] = bids
			}
			asks, ok := p.asks[pair]
			if !ok {
				asks = internal.NewPriceBook()
				p.asks[pair] = asks
			}

			processor := func(price, count, amount float64) {
				isBid := amount > 0
				if isBid {
					if count == 0 {
						bids.RemoveByPrice(price)
					} else {
						bids.Upsert(price, math.Abs(amount))
					}
				} else {
					if count == 0 {
						asks.RemoveByPrice(price)
					} else {
						asks.Upsert(price, math.Abs(amount))
					}
				}
			}

			switch records := msg[1].(type) {
			case []any:
				if len(records) > 0 {
					switch records[0].(type) {
					case float64:
						processor(records[0].(float64), records[1].(float64), records[2].(float64))
					case []any:
						for _, r := range records {
							record := r.([]any)
							processor(record[0].(float64), record[1].(float64), record[2].(float64))
						}
					}
					p.onTick(pair, bids.GetMin(), asks.GetMax())
				}
			case string:
			default:
				p.logger.Println("Unknown message:", msg[1])
			}

		}
	default:
		p.logger.Println("Unknown message:", msg)
	}
}

func (p *TickerProcessor) onTick(pair string, bid, ask float64) {
	now := time.Now().UnixMilli()

	if last, ok := p.last[pair]; ok {
		if last.Bid == bid && last.Ask == ask && now-last.TimestampMs < 10000 {
			return
		}
	}

	uuid, _ := uuid.NewV7()

	tick := types.Tick{
		SymbolId:    pair,
		Bid:         bid,
		Ask:         ask,
		TimestampMs: now,
		Provider:    "bitfinex",
		TickId:      uuid.String(),
	}
	p.last[pair] = tick

	p.tickCh <- tick
}

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)

	logger.Println("Starting Bitfinex connector")
	defer logger.Println("Bitfinex connector stopped")

	ctx, cancel := context.WithCancel(context.Background())

	logger.Println("Connecting to NATS")
	natsClient, err := nats.Connect("localhost:4222")
	if err != nil {
		logger.Fatal("Failed to connect to NATS: ", err)
	}

	logger.Println("Connecting to NATS JetStream")
	js, err := natsClient.JetStream()
	if err != nil {
		logger.Fatal("Failed to connect to NATS: ", err)
	}

	ws := NewTickerProcessor(logger, func() internal.SymbolsProvider {
		return internal.NewSymbolRepository(logger, js, "symbols-bitfinex")
	})
	ws.Run(ctx)

	go func() {
		defer logger.Println("Ticks consumer stopped")
		for {
			select {
			case <-ctx.Done():
				return

			case tick := <-ws.Ticks():
				logger.Println("Publishing tick:", tick)
				json, err := json.Marshal(tick)
				if err != nil {
					logger.Fatal("Failed to marshal tick: ", err)
					continue
				}
				subject := fmt.Sprintf("ticks-feed-bitfinex.%s", tick.SymbolId)
				natsClient.Publish(subject, json)
			}
		}
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	cancel()

	ws.Wait(context.Background())
}
