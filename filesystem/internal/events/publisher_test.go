//nolint:testpackage // tests exercise the internal publisher wire helper.
package events

import (
	"context"
	"errors"
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

	publisher, err := NewPublisher(ctx, endpoint, Options{})
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

	if err := publisher.PublishBlocksRemoved(ctx, BlocksRemoved{ModelName: "test-model", BlockHashes: []uint64{42}}); err != nil {
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

	publisher, err := NewPublisher(ctx, endpoint, Options{})
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
		if err := publisher.PublishBlocksRemoved(ctx, BlocksRemoved{ModelName: "test-model", BlockHashes: []uint64{blockHash}}); err != nil {
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

	publisher, err := NewPublisher(ctx, endpoint, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatal(err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("second Close returned %v, want nil", err)
	}
	if err := publisher.PublishBlocksRemoved(ctx, BlocksRemoved{ModelName: "test-model", BlockHashes: []uint64{1}}); err != nil {
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
		"tcp://*:5570":                    true,
		"tcp://[::]:5570":                 true,
		"ipc:///tmp/events":               true,
		"inproc://events":                 true,
		"tcp://epp.llm-d.svc:5570":        false,
		"tcp://10.0.0.7:5570":             false,
		"tcp://epp-0.epp.llm-d.svc:5570":  false,
		"tcp://[fd00::7]:5570":            false,
		"tcp://0.0.0.0:5570":              true,
		"tcp://localhost.localdomain:557": false,
	} {
		if got := isBindEndpoint(endpoint); got != want {
			t.Errorf("isBindEndpoint(%q) = %v, want %v", endpoint, got, want)
		}
	}
}

func TestNewPublisherRejectsBindEndpointInList(t *testing.T) {
	if _, err := NewPublisher(context.Background(), "tcp://epp:5570,tcp://*:5571", Options{}); err == nil {
		t.Fatal("bind endpoint in a list was accepted")
	}
}

// receive collects messages from subscriber until it has want of them.
func receive(t *testing.T, ctx context.Context, subscriber zmq4.Socket, want int) []zmq4.Msg {
	t.Helper()
	messages := make(chan zmq4.Msg, want)
	go func() {
		for range want {
			message, err := subscriber.Recv()
			if err != nil {
				return
			}
			messages <- message
		}
	}()
	var got []zmq4.Msg
	for len(got) < want {
		select {
		case message := <-messages:
			got = append(got, message)
		case <-ctx.Done():
			t.Fatalf("received %d messages, want %d", len(got), want)
		}
	}
	return got
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

// The router's global socket binds, so the evictor must connect to it.
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

	publisher, err := NewPublisher(ctx, endpoint, Options{})
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
			if err := publisher.PublishBlocksRemoved(ctx, BlocksRemoved{ModelName: "m", BlockHashes: []uint64{hash}}); err != nil {
				t.Fatal(err)
			}
			select {
			case message := <-got:
				if hashes := decodeEvent(t, message.Frames[2])[1].([]any); hashes[0] != hash {
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

func TestPublisherSendsOneStorageEventPerPod(t *testing.T) {
	endpoint := "inproc://pvc-evictor-per-pod-test"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	publisher, err := NewPublisher(ctx, endpoint, Options{
		PodHosts:   []string{"10.0.0.6", "10.0.0.5"},
		PodPorts:   []string{"8000"},
		ModelNames: map[string]string{"/models/qwen3-8b": "Qwen/Qwen3-8B"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = publisher.Close() }()

	subscriber := zmq4.NewSub(ctx)
	defer func() { _ = subscriber.Close() }()
	if err := subscriber.SetOption(zmq4.OptionSubscribe, "kv@"); err != nil {
		t.Fatal(err)
	}
	if err := subscriber.Dial(endpoint); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	removed := BlocksRemoved{ModelName: "/models/qwen3-8b", GroupIdx: 1, HasGroup: true, BlockHashes: []uint64{7, 8}}
	if err := publisher.PublishBlocksRemoved(ctx, removed); err != nil {
		t.Fatal(err)
	}

	messages := receive(t, ctx, subscriber, 2)
	for i, wantTopic := range []string{"kv@10.0.0.5:8000@Qwen/Qwen3-8B", "kv@10.0.0.6:8000@Qwen/Qwen3-8B"} {
		if topic := string(messages[i].Frames[0]); topic != wantTopic {
			t.Fatalf("message %d topic = %q, want %q", i, topic, wantTopic)
		}
		event := decodeEvent(t, messages[i].Frames[2])
		hashes, _ := event[1].([]any)
		if len(event) != 4 || event[0] != "BlockRemoved" || event[2] != "STORAGE" ||
			len(hashes) != 2 || hashes[0] != uint64(7) || hashes[1] != uint64(8) {
			t.Fatalf("message %d event = %#v", i, event)
		}
		if group, ok := event[3].(int8); !ok || group != 1 {
			t.Fatalf("message %d group_idx = %#v, want 1", i, event[3])
		}
	}
}

func TestPublisherRejectsPerPodEventWithoutGroup(t *testing.T) {
	ctx := context.Background()
	publisher, err := NewPublisher(ctx, "inproc://pvc-evictor-no-group-test", Options{
		PodHosts: []string{"10.0.0.5"},
		PodPorts: []string{"8000"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = publisher.Close() }()

	err = publisher.PublishBlocksRemoved(ctx, BlocksRemoved{ModelName: "m", BlockHashes: []uint64{1}})
	if !errors.Is(err, ErrNoGroup) {
		t.Fatalf("error = %v, want ErrNoGroup", err)
	}
}
