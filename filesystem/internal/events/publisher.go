package events

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	zmq4 "github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	sendHighWaterMark = 100000

	// closeDrainDelay is how long Close lets a recent event leave the queue.
	closeDrainDelay = 500 * time.Millisecond

	// dialRetryInterval is the wait between two attempts to reach a connect
	// endpoint, at startup and after a lost connection.
	dialRetryInterval = 500 * time.Millisecond
)

type Publisher struct {
	socket      zmq4.Socket
	stopDialing context.CancelFunc
	dialing     sync.WaitGroup
	sequence    uint64
	lastPublish time.Time
	mutex       sync.Mutex
}

// NewPublisher opens the PUB socket. An endpoint with a wildcard host
// (tcp://*:5570) or an ipc:// or inproc:// address is bound, as vLLM's
// ZmqEventPublisher binds tcp://*:5557. Any other endpoint is connected to.
// Connecting does not wait for the peer: the socket keeps retrying in the
// background and reconnects after a lost connection. Events sent while no
// peer is connected are dropped.
func NewPublisher(ctx context.Context, endpoint string) (*Publisher, error) {
	if isBindEndpoint(endpoint) {
		socket := zmq4.NewPub(ctx)
		if err := socket.SetOption(zmq4.OptionHWM, sendHighWaterMark); err != nil {
			_ = socket.Close()
			return nil, fmt.Errorf("set ZMQ send high-water mark: %w", err)
		}
		if err := socket.Listen(endpoint); err != nil {
			_ = socket.Close()
			return nil, fmt.Errorf("listen on ZMQ endpoint %q: %w", endpoint, err)
		}
		return &Publisher{socket: socket}, nil
	}

	dialCtx, stopDialing := context.WithCancel(ctx)
	socket := zmq4.NewPub(dialCtx,
		zmq4.WithDialerRetry(dialRetryInterval),
		zmq4.WithDialerMaxRetries(-1),
		zmq4.WithAutomaticReconnect(true),
	)
	if err := socket.SetOption(zmq4.OptionHWM, sendHighWaterMark); err != nil {
		stopDialing()
		_ = socket.Close()
		return nil, fmt.Errorf("set ZMQ send high-water mark: %w", err)
	}
	publisher := &Publisher{socket: socket, stopDialing: stopDialing}
	publisher.dialing.Add(1)
	go func() {
		defer publisher.dialing.Done()
		for dialCtx.Err() == nil {
			if socket.Dial(endpoint) == nil {
				return
			}
			select {
			case <-dialCtx.Done():
			case <-time.After(dialRetryInterval):
			}
		}
	}()
	return publisher, nil
}

// isBindEndpoint reports whether endpoint is an ipc:// or inproc:// address
// or a TCP address with a wildcard host: *, 0.0.0.0, or [::].
func isBindEndpoint(endpoint string) bool {
	if strings.HasPrefix(endpoint, "ipc://") || strings.HasPrefix(endpoint, "inproc://") {
		return true
	}
	address, ok := strings.CutPrefix(endpoint, "tcp://")
	if !ok {
		return false
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	return host == "*" || host == "0.0.0.0" || host == "::"
}

func (p *Publisher) PublishBlocksRemoved(ctx context.Context, blockHashes []uint64, modelName string) error {
	if len(blockHashes) == 0 || modelName == "" {
		return nil
	}

	event, err := msgpack.Marshal([]any{"BlockRemoved", blockHashes, "SHARED_STORAGE"})
	if err != nil {
		return fmt.Errorf("encode BlockRemoved event: %w", err)
	}
	payload, err := msgpack.Marshal([]any{float64(time.Now().UnixNano()) / 1e9, []msgpack.RawMessage{event}, nil})
	if err != nil {
		return fmt.Errorf("encode BlockRemoved batch: %w", err)
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.socket == nil {
		return nil
	}
	p.sequence++
	sequence := make([]byte, binary.Size(uint64(0)))
	binary.BigEndian.PutUint64(sequence, p.sequence)
	topic := "kv@SHARED_STORAGE@" + modelName

	err = p.socket.Send(zmq4.NewMsgFrom([]byte(topic), sequence, payload))
	p.lastPublish = time.Now()
	return err
}

// Close shuts the socket down. If an event was published less than
// closeDrainDelay ago, it first waits out the rest of that time.
//
// Send only queues the event. The socket sends it later from a background
// goroutine, and closing the socket drops anything still queued. The ZMQ
// library has no flush call, so this wait is a best-effort way to let the
// final BlockRemoved events leave before shutdown.
func (p *Publisher) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.socket == nil {
		return nil
	}
	if wait := closeDrainDelay - time.Since(p.lastPublish); wait > 0 {
		time.Sleep(wait)
	}
	if p.stopDialing != nil {
		p.stopDialing()
	}
	socket := p.socket
	p.socket = nil
	err := socket.Close()
	p.dialing.Wait()
	return err
}
