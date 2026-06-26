package client

import (
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"

	vaultfsv1 "github.com/sumanthd032/vaultfs/proto/vaultfs/v1"
)

// errNoMaster is returned when no configured master address is reachable.
var errNoMaster = errors.New("client: no reachable master")

// pool manages and caches gRPC connections to master and chunk-server
// addresses. It is safe for concurrent use.
type pool struct {
	masterAddrs []string
	dialOpts    []grpc.DialOption

	mu    sync.Mutex // protects conns
	conns map[string]*grpc.ClientConn
}

func newPool(masterAddrs []string, dialOpts []grpc.DialOption) *pool {
	return &pool{
		masterAddrs: masterAddrs,
		dialOpts:    dialOpts,
		conns:       make(map[string]*grpc.ClientConn),
	}
}

// conn returns a cached connection to addr, dialing lazily on first use.
func (p *pool) conn(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr, p.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", addr, err)
	}
	p.conns[addr] = c
	return c, nil
}

// chunkClient returns a ChunkServiceClient for a chunk server at addr.
func (p *pool) chunkClient(addr string) (vaultfsv1.ChunkServiceClient, error) {
	c, err := p.conn(addr)
	if err != nil {
		return nil, err
	}
	return vaultfsv1.NewChunkServiceClient(c), nil
}

// close shuts down all cached connections.
func (p *pool) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error
	for addr, c := range p.conns {
		if err := c.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close %s: %w", addr, err))
		}
	}
	p.conns = make(map[string]*grpc.ClientConn)
	return errors.Join(errs...)
}
