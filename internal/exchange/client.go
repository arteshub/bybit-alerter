package exchange

import "context"

type Fetcher interface {
	FetchSymbols(ctx context.Context) ([]Symbol, error)
	FetchAvgTurnover(ctx context.Context, sym Symbol, days int) (float64, error)
}

type Subscriber interface {
	Subscribe(ctx context.Context, symbols []Symbol, handler func(Candle)) error
}
