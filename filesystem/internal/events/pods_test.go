//nolint:testpackage // tests exercise the internal pod resolver.
package events

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestPodResolverEndpoints(t *testing.T) {
	resolver := newPodResolver([]string{"vllm-pods", "10.0.0.9", "fd00::7"}, []string{"8000", "8001"})
	resolver.lookupHost = func(_ context.Context, host string) ([]string, error) {
		if host != "vllm-pods" {
			t.Fatalf("looked up %q", host)
		}
		return []string{"10.0.0.6", "10.0.0.5", "10.0.0.9"}, nil
	}

	got, err := resolver.endpoints(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// The router names a pod fmt.Sprintf("%s:%s", address, port), so the IPv6
	// identifier has no brackets.
	want := []string{
		"10.0.0.5:8000", "10.0.0.5:8001",
		"10.0.0.6:8000", "10.0.0.6:8001",
		"10.0.0.9:8000", "10.0.0.9:8001",
		"fd00::7:8000", "fd00::7:8001",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("endpoints = %v, want %v", got, want)
	}
}

func TestPodResolverCachesAnswer(t *testing.T) {
	now := time.Unix(1000, 0)
	lookups := 0
	resolver := newPodResolver([]string{"vllm-pods"}, []string{"8000"})
	resolver.now = func() time.Time { return now }
	resolver.lookupHost = func(context.Context, string) ([]string, error) {
		lookups++
		return []string{"10.0.0.5"}, nil
	}

	for range 3 {
		if _, err := resolver.endpoints(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if lookups != 1 {
		t.Fatalf("lookups within TTL = %d, want 1", lookups)
	}
	now = now.Add(podCacheTTL)
	if _, err := resolver.endpoints(context.Background()); err != nil {
		t.Fatal(err)
	}
	if lookups != 2 {
		t.Fatalf("lookups after TTL = %d, want 2", lookups)
	}
}

func TestPodResolverReturnsResolvedHostsWithError(t *testing.T) {
	lookups := 0
	resolver := newPodResolver([]string{"missing", "10.0.0.5"}, []string{"8000"})
	resolver.lookupHost = func(context.Context, string) ([]string, error) {
		lookups++
		return nil, errors.New("no such host")
	}

	got, err := resolver.endpoints(context.Background())
	if err == nil {
		t.Fatal("missing host gave no error")
	}
	if !slices.Equal(got, []string{"10.0.0.5:8000"}) {
		t.Fatalf("endpoints = %v, want [10.0.0.5:8000]", got)
	}
	// A failed answer is not cached.
	if _, err := resolver.endpoints(context.Background()); err == nil || lookups != 2 {
		t.Fatalf("second call: err=%v lookups=%d, want an error and 2 lookups", err, lookups)
	}
}
