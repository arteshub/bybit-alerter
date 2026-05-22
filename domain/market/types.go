package market

type Symbol string

type Candle struct {
	Symbol   Symbol
	Turnover float64
	Date     string
	IsClosed bool
}
