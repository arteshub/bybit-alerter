package bybit

import (
	"sync"

	sdk "github.com/hirokisan/bybit/v2"
)

type Client struct {
	rest    *sdk.Client
	ws      *sdk.WebSocketClient
	delayMS int
	wg      sync.WaitGroup
}

func NewClient(delayMS int) *Client {
	return &Client{
		rest:    sdk.NewClient(),
		ws:      sdk.NewWebsocketClient(),
		delayMS: delayMS,
	}
}
