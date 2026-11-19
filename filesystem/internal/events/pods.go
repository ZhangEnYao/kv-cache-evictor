package events

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
)

// podResolver turns the configured pod hosts into the identifiers the
// llm-d router's KV-cache index uses for vLLM pods. The router's pod discovery
// names a pod "<pod IP>:<serving port>" (fmt.Sprintf("%s:%s", address, port)),
// so IPv6 addresses carry no brackets here either.
type podResolver struct {
	hosts      []string
	ports      []string
	lookupHost func(ctx context.Context, host string) ([]string, error)
}

func newPodResolver(hosts, ports []string) *podResolver {
	return &podResolver{
		hosts:      hosts,
		ports:      ports,
		lookupHost: net.DefaultResolver.LookupHost,
	}
}

// endpoints returns one identifier per pod IP and port. A host that is an IP
// address is used as is; any other host is resolved with DNS, so a headless
// Service name yields every ready pod behind it. When a host cannot be
// resolved, the identifiers of the other hosts are still returned with the
// error.
func (r *podResolver) endpoints(ctx context.Context) ([]string, error) {
	seen := make(map[string]struct{})
	var ips []string
	add := func(address string) {
		if ip := net.ParseIP(address); ip != nil {
			address = ip.String()
		}
		if _, ok := seen[address]; !ok {
			seen[address] = struct{}{}
			ips = append(ips, address)
		}
	}
	var errs []error
	for _, host := range r.hosts {
		if net.ParseIP(host) != nil {
			add(host)
			continue
		}
		addresses, err := r.lookupHost(ctx, host)
		if err != nil {
			errs = append(errs, fmt.Errorf("resolve pod host %q: %w", host, err))
			continue
		}
		for _, address := range addresses {
			add(address)
		}
	}
	sort.Strings(ips)

	endpoints := make([]string, 0, len(ips)*len(r.ports))
	for _, ip := range ips {
		for _, port := range r.ports {
			endpoints = append(endpoints, ip+":"+port)
		}
	}
	return endpoints, errors.Join(errs...)
}
