//nolint:testpackage // tests exercise the internal publisher wire helper.
package events

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	zmq4 "github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

func TestPublisherSendsBlockRemovedEvent(t *testing.T) {
	endpoint := "inproc://pvc-evictor-block-removed-test"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	publisher, err := NewPublisher(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = publisher.Close() }()

	subscriber := zmq4.NewSub(ctx)
	defer func() { _ = subscriber.Close() }()
	if err := subscriber.SetOption(zmq4.OptionSubscribe, "kv@SHARED_STORAGE@test-model"); err != nil {
		t.Fatal(err)
	}
	if err := subscriber.Dial(endpoint); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if err := publisher.PublishBlocksRemoved(ctx, []uint64{42}, "test-model"); err != nil {
		t.Fatal(err)
	}

	messages := make(chan zmq4.Msg, 1)
	errors := make(chan error, 1)
	go func() {
		message, err := subscriber.Recv()
		if err != nil {
			errors <- err
			return
		}
		messages <- message
	}()

	select {
	case message := <-messages:
		assertBlockRemovedMessage(t, message)
	case err := <-errors:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for storage event")
	}
}

// The service closes the publisher right after the deleter's final flush.
// Events published just before Close must still reach the subscriber.
func TestCloseDeliversRecentlyQueuedEvents(t *testing.T) {
	endpoint := "inproc://pvc-evictor-close-drain-test"
	const wantEvents = 3

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	publisher, err := NewPublisher(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}

	subscriber := zmq4.NewSub(ctx)
	defer func() { _ = subscriber.Close() }()
	if err := subscriber.SetOption(zmq4.OptionSubscribe, "kv@SHARED_STORAGE@test-model"); err != nil {
		t.Fatal(err)
	}
	if err := subscriber.Dial(endpoint); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	var received atomic.Int64
	go func() {
		for {
			if _, err := subscriber.Recv(); err != nil {
				return
			}
			received.Add(1)
		}
	}()

	for blockHash := range uint64(wantEvents) {
		if err := publisher.PublishBlocksRemoved(ctx, []uint64{blockHash}, "test-model"); err != nil {
			t.Fatal(err)
		}
	}
	if err := publisher.Close(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for received.Load() < wantEvents && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := received.Load(); got != wantEvents {
		t.Fatalf("subscriber received %d events, want %d", got, wantEvents)
	}
}

func TestCloseIsIdempotentAndStopsPublishing(t *testing.T) {
	endpoint := "inproc://pvc-evictor-close-twice-test"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	publisher, err := NewPublisher(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatal(err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("second Close returned %v, want nil", err)
	}
	if err := publisher.PublishBlocksRemoved(ctx, []uint64{1}, "test-model"); err != nil {
		t.Fatalf("publish after Close returned %v, want nil", err)
	}
}

func assertBlockRemovedMessage(t *testing.T, message zmq4.Msg) {
	t.Helper()
	if len(message.Frames) != 3 {
		t.Fatalf("got %d frames, want 3", len(message.Frames))
	}
	if topic := string(message.Frames[0]); topic != "kv@SHARED_STORAGE@test-model" {
		t.Fatalf("topic = %q", topic)
	}
	if sequence := message.Frames[1]; len(sequence) != 8 || sequence[7] != 1 {
		t.Fatalf("sequence = %v, want big-endian 1", sequence)
	}

	var batch []any
	if err := msgpack.Unmarshal(message.Frames[2], &batch); err != nil {
		t.Fatal(err)
	}
	if len(batch) != 3 {
		t.Fatalf("batch has %d fields, want 3", len(batch))
	}
	if batch[2] != nil {
		t.Fatalf("data parallel rank = %#v, want nil", batch[2])
	}
	rawEvents, ok := batch[1].([]any)
	if !ok || len(rawEvents) != 1 {
		t.Fatalf("events = %#v, want one event", batch[1])
	}
	event, ok := rawEvents[0].([]any)
	if !ok {
		t.Fatalf("event = %T, want []any", rawEvents[0])
	}
	if len(event) != 3 || event[0] != "BlockRemoved" || event[2] != "SHARED_STORAGE" {
		t.Fatalf("event = %#v", event)
	}
	hashes, ok := event[1].([]any)
	if !ok || len(hashes) != 1 || hashes[0] != uint64(42) {
		t.Fatalf("hashes = %#v, want [42]", event[1])
	}
}

func TestIsBindEndpoint(t *testing.T) {
	for endpoint, want := range map[string]bool{
		"tcp://*:5570":                   true,
		"tcp://0.0.0.0:5570":             true,
		"tcp://[::]:5570":                true,
		"ipc:///tmp/events":              true,
		"inproc://events":                true,
		"tcp://epp.llm-d.svc:5570":       false,
		"tcp://10.0.0.7:5570":            false,
		"tcp://epp-0.epp.llm-d.svc:5570": false,
		"tcp://[fd00::7]:5570":           false,
	} {
		if got := isBindEndpoint(endpoint); got != want {
			t.Errorf("isBindEndpoint(%q) = %v, want %v", endpoint, got, want)
		}
	}
}

func decodeEvent(t *testing.T, payload []byte) []any {
	t.Helper()
	var batch []any
	if err := msgpack.Unmarshal(payload, &batch); err != nil {
		t.Fatal(err)
	}
	rawEvents, ok := batch[1].([]any)
	if !ok || len(rawEvents) != 1 {
		t.Fatalf("events = %#v, want one event", batch[1])
	}
	event, ok := rawEvents[0].([]any)
	if !ok {
		t.Fatalf("event = %T, want []any", rawEvents[0])
	}
	return event
}

// The llm-d router's kvEventsConfig.zmqEndpoint binds, so the evictor must
// connect to it.
func TestPublisherConnectsToBoundSubscriberAndReconnects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	subscriber := zmq4.NewSub(ctx)
	if err := subscriber.SetOption(zmq4.OptionSubscribe, "kv@"); err != nil {
		t.Fatal(err)
	}
	if err := subscriber.Listen("tcp://127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	endpoint := "tcp://" + subscriber.Addr().String()

	publisher, err := NewPublisher(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = publisher.Close() }()

	publishUntilReceived := func(sub zmq4.Socket, hash uint64) {
		t.Helper()
		got := make(chan zmq4.Msg, 1)
		go func() {
			if message, err := sub.Recv(); err == nil {
				got <- message
			}
		}()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			if err := publisher.PublishBlocksRemoved(ctx, []uint64{hash}, "test-model"); err != nil {
				t.Fatal(err)
			}
			select {
			case message := <-got:
				hashes, _ := decodeEvent(t, message.Frames[2])[1].([]any)
				if len(hashes) != 1 || hashes[0] != hash {
					t.Fatalf("hashes = %v, want [%d]", hashes, hash)
				}
				return
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal("timeout waiting for the connected publisher")
			}
		}
	}
	publishUntilReceived(subscriber, 1)

	// A router restart: the bound socket goes away and comes back on the
	// same address.
	_ = subscriber.Close()
	restarted := zmq4.NewSub(ctx)
	defer func() { _ = restarted.Close() }()
	if err := restarted.SetOption(zmq4.OptionSubscribe, "kv@"); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Listen(endpoint); err != nil {
		t.Fatal(err)
	}
	publishUntilReceived(restarted, 2)
}
