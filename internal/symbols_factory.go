package internal

import (
	"context"
)

type SymbolsProvider interface {
	Listen(ctx context.Context) (chan Symbol, chan string)
}
type SymbolsProviderFunc func() SymbolsProvider
