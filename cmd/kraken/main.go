package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
		"method": "subscribe",
		"params": map[string]any{
			"channel": "book",
			"symbol":  []string{symbol.ProviderId},
		},
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

	p.commands <- map[string]any{
		"method": "unsubscribe",
		"params": map[string]any{
			"channel": "book",
			"symbol":  []string{symbol.ProviderId},
		},
	}
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
	messages, connect, err := p.ws.Connect(ctx, "wss://ws.kraken.com/v2")
	if err != nil {
		p.logger.Println("Error connecting to Kraken: ", err)
		return
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		for range connect {
			p.logger.Println("Connected to Kraken")
			p.OnConnect(ctx)
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

func (p *TickerProcessor) OnConnect(ctx context.Context) {
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

func (p *TickerProcessor) process(_ context.Context, m internal.WSMessage) {
	msg, ok := m.(map[string]any)
	if !ok {
		p.logger.Println("Unknown message type:", m)
		return
	}

	switch msg["method"] {
	case "subscribe":
		p.logger.Println("Subscribe message:", msg["result"])
		return
	case "unsubscribe":
		p.logger.Println("Unsubscribe message:", msg["result"])
		return
	}

	switch msg["channel"] {
	case "status":
		p.logger.Println("Status message:", msg)
	case "book":
		switch msg["type"] {
		case "snapshot", "update":

			array := msg["data"].([]any)
			for _, data := range array {
				dataMap := data.(map[string]any)

				symbol := p.getSymbolByProviderId(dataMap["symbol"].(string))
				if symbol == nil {
					continue
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

				for _, entry := range dataMap["bids"].([]any) {
					entryMap := entry.(map[string]any)
					price := entryMap["price"].(float64)
					qty := entryMap["qty"].(float64)
					if qty == 0 {
						bids.RemoveByPrice(price)
					} else {
						bids.Upsert(price, qty)
					}
				}

				for _, entry := range dataMap["asks"].([]any) {
					entryMap := entry.(map[string]any)
					price := entryMap["price"].(float64)
					qty := entryMap["qty"].(float64)
					if qty == 0 {
						asks.RemoveByPrice(price)
					} else {
						asks.Upsert(price, qty)
					}
				}

				p.onTick(pair, bids.GetMin(), asks.GetMax())
			}
		default:
			p.logger.Println("Unknown type:", msg["type"])
		}
	case "heartbeat":
	default:
		p.logger.Println("Unknown channel:", msg)
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
		Provider:    "kraken",
		TickId:      uuid.String(),
	}
	p.last[pair] = tick

	p.tickCh <- tick
}

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)

	logger.Println("Starting Kraken connector")
	defer logger.Println("Kraken connector stopped")

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
		return internal.NewSymbolRepository(logger, js, "symbols-kraken")
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
				subject := fmt.Sprintf("ticks-feed-kraken.%s", tick.SymbolId)
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
