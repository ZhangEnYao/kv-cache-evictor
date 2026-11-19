package events

import (
	"context"
	"encoding/binary"
	"errors"
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

	// sharedStorage is the pod and medium of llm-d's shared-storage events.
	sharedStorage = "SHARED_STORAGE"

	// storageMedium is the medium of the BlockStored events that vLLM's
	// file-system offloading tier publishes. The router stores it lowercased
	// as the device tier of the index entry.
	storageMedium = "STORAGE"
)

// ErrNoGroup is returned for a per-pod event whose files have no KV-cache
// group in their path. vLLM's STORAGE entries always carry a group, and the
// router only removes an entry whose group matches.
var ErrNoGroup = errors.New("cache file path has no _g<group> directory")

// BlocksRemoved describes the cache files of one model and one KV-cache group
// that were deleted together.
type BlocksRemoved struct {
	ModelName string
	// GroupIdx is the KV-cache group from the file's <hh>_g<group> directory.
	// HasGroup is false when the path has no such directory.
	GroupIdx    int
	HasGroup    bool
	BlockHashes []uint64
}

// Options selects how events are addressed.
type Options struct {
	// PodHosts names the vLLM pods that use the cache. When it is empty, events
	// go to the kv@SHARED_STORAGE@<model> topic with medium SHARED_STORAGE.
	// When it is set, every event is sent once per pod and serving port, with
	// the topic, medium and group of the STORAGE events the pod published.
	PodHosts []string
	PodPorts []string
}

type Publisher struct {
	socket      zmq4.Socket
	stopDialing context.CancelFunc
	dialing     sync.WaitGroup
	pods        *podResolver
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
func NewPublisher(ctx context.Context, endpoint string, options Options) (*Publisher, error) {
	var pods *podResolver
	if len(options.PodHosts) > 0 {
		pods = newPodResolver(options.PodHosts, options.PodPorts)
	}

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
		return &Publisher{socket: socket, pods: pods}, nil
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
	publisher := &Publisher{socket: socket, stopDialing: stopDialing, pods: pods}
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

// PublishBlocksRemoved sends one BlockRemoved event for removed. With pod
// hosts configured, the event is sent to every pod; an error from resolving
// some of the hosts is returned after the event was sent to the others.
func (p *Publisher) PublishBlocksRemoved(ctx context.Context, removed BlocksRemoved) error {
	if len(removed.BlockHashes) == 0 || removed.ModelName == "" {
		return nil
	}

	if p.pods == nil {
		event := []any{"BlockRemoved", removed.BlockHashes, sharedStorage}
		return p.send([]string{"kv@" + sharedStorage + "@" + removed.ModelName}, event)
	}

	if !removed.HasGroup {
		return ErrNoGroup
	}
	pods, resolveErr := p.pods.endpoints(ctx)
	topics := make([]string, 0, len(pods))
	for _, pod := range pods {
		topics = append(topics, "kv@"+pod+"@"+removed.ModelName)
	}
	event := []any{"BlockRemoved", removed.BlockHashes, storageMedium, removed.GroupIdx}
	return errors.Join(resolveErr, p.send(topics, event))
}

func (p *Publisher) send(topics []string, event []any) error {
	if len(topics) == 0 {
		return nil
	}
	encodedEvent, err := msgpack.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode BlockRemoved event: %w", err)
	}
	payload, err := msgpack.Marshal([]any{float64(time.Now().UnixNano()) / 1e9, []msgpack.RawMessage{encodedEvent}, nil})
	if err != nil {
		return fmt.Errorf("encode BlockRemoved batch: %w", err)
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.socket == nil {
		return nil
	}
	var errs []error
	for _, topic := range topics {
		p.sequence++
		sequence := make([]byte, binary.Size(uint64(0)))
		binary.BigEndian.PutUint64(sequence, p.sequence)
		if err := p.socket.Send(zmq4.NewMsgFrom([]byte(topic), sequence, payload)); err != nil {
			errs = append(errs, err)
		}
	}
	p.lastPublish = time.Now()
	return errors.Join(errs...)
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
