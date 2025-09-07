package internal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const (
	reconnectTimeout = 15 * time.Second
	readTimeout      = 35 * time.Second
	writeTimeout     = 11 * time.Second
)

type WSConnect any
type WSMessage any
type WSCommand any

type WS struct {
	connect  chan WSConnect
	messages chan WSMessage
	commands chan WSCommand
	conn     *websocket.Conn
}

func NewWS(commands chan WSCommand) *WS {
	return &WS{
		connect:  make(chan WSConnect),
		commands: commands,
		messages: make(chan WSMessage, 100),
		conn:     nil,
	}
}

func (w *WS) Connect(ctx context.Context, url string) (chan WSMessage, chan WSConnect, error) {
	go func() {
		fmt.Println("WS worker started")
		defer fmt.Println("WS worker stopped")

		defer close(w.messages)
		defer close(w.connect)

		for {
			fmt.Println("Dialing up")

			w.run(ctx, url)
			if ctx.Err() != nil {
				break
			}

			time.Sleep(reconnectTimeout)
		}
	}()

	return w.messages, w.connect, nil
}

func (w *WS) SendCommand(cmd WSCommand) {
	w.commands <- cmd
}

func (w *WS) ReadMessage() (WSMessage, bool) {
	msg, ok := <-w.messages
	return msg, ok
}

func (w *WS) run(ctx context.Context, url string) {
	// inner cancellable context for correct re dial
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		fmt.Println("Error connecting to WS: ", err)
		return
	}
	w.conn = conn

	defer w.conn.Close(websocket.StatusNormalClosure, "normal closure")

	w.connect <- true

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		fmt.Println("Messages reader started")
		defer fmt.Println("Messages reader stopped")

		defer wg.Done()
		defer cancel()

		for {
			ctx, cancel := context.WithTimeout(ctx, readTimeout)
			defer cancel()

			var msg WSMessage
			err := wsjson.Read(ctx, w.conn, &msg)
			if err != nil {
				fmt.Println("Error reading message from WS: ", err)
				return
			}
			w.messages <- msg
		}
	}()

	wg.Add(1)
	go func() {
		fmt.Println("Commands writer started")
		defer fmt.Println("Commands writer stopped")

		defer wg.Done()
		defer cancel()

		for {
			select {
			case cmd, ok := <-w.commands:
				if !ok {
					fmt.Println("Commands channel closed")
					return
				}

				ctx, cancel := context.WithTimeout(ctx, writeTimeout)
				defer cancel()

				err := wsjson.Write(ctx, w.conn, cmd)
				if err != nil {
					fmt.Println("Error writing command to WS: ", err)
					return
				}
			case <-ctx.Done():
				fmt.Println("Context done")
				return
			}
		}
	}()

	wg.Wait()
}
