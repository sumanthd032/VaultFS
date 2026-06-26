// Package client is the public Go SDK for VaultFS. It connects to the master
// cluster, splits files into content-addressed chunks, writes them to chunk
// servers along a replication chain, and reads them back with integrity
// verification.
//
// Typical use:
//
//	c, err := client.New(client.Config{MasterAddrs: []string{"localhost:9000"}})
//	if err != nil { ... }
//	defer c.Close()
//	err = c.Put(ctx, "./local.txt", "/remote/local.txt")
package client

import (
	"context"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	vaultfsv1 "github.com/sumanthd032/vaultfs/proto/vaultfs/v1"
)

// Config configures a Client.
type Config struct {
	// MasterAddrs is one or more master addresses. The client fails over across
	// them in order when a master is unreachable.
	MasterAddrs []string
	// ChunkSize overrides the file split size. Zero uses DefaultChunkSize.
	ChunkSize int
	// DialOptions overrides the gRPC dial options. When nil, an insecure
	// transport is used (mTLS is wired in Step 5).
	DialOptions []grpc.DialOption
}

// Client is a connection to a VaultFS cluster. It is safe for concurrent use.
type Client struct {
	pool      *pool
	chunkSize int
	retry     retryPolicy
}

// New creates a Client from cfg. It returns an error if no master addresses are
// configured.
func New(cfg Config) (*Client, error) {
	if len(cfg.MasterAddrs) == 0 {
		return nil, errors.New("client: at least one master address is required")
	}
	dialOpts := cfg.DialOptions
	if dialOpts == nil {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &Client{
		pool:      newPool(cfg.MasterAddrs, dialOpts),
		chunkSize: chunkSize,
		retry:     defaultRetryPolicy(),
	}, nil
}

// Close releases all connections held by the client.
func (c *Client) Close() error { return c.pool.close() }

// callMaster runs fn against the master cluster with retry and failover: it
// tries each master address in order, retrying transient errors on the current
// master before moving to the next. Non-transient errors are returned
// immediately without trying other masters.
func (c *Client) callMaster(ctx context.Context, fn func(vaultfsv1.MasterServiceClient) error) error {
	var lastErr error
	for _, addr := range c.pool.masterAddrs {
		if err := ctx.Err(); err != nil {
			return err
		}
		cc, err := c.pool.conn(addr)
		if err != nil {
			lastErr = err
			continue
		}
		m := vaultfsv1.NewMasterServiceClient(cc)
		err = c.retry.do(ctx, func() error { return fn(m) })
		if err == nil {
			return nil
		}
		lastErr = err
		if !isTransient(err) {
			return err
		}
	}
	if lastErr == nil {
		lastErr = errNoMaster
	}
	return lastErr
}

// callAdmin runs fn against the admin service with the same failover behaviour
// as callMaster.
func (c *Client) callAdmin(ctx context.Context, fn func(vaultfsv1.AdminServiceClient) error) error {
	var lastErr error
	for _, addr := range c.pool.masterAddrs {
		if err := ctx.Err(); err != nil {
			return err
		}
		cc, err := c.pool.conn(addr)
		if err != nil {
			lastErr = err
			continue
		}
		a := vaultfsv1.NewAdminServiceClient(cc)
		err = c.retry.do(ctx, func() error { return fn(a) })
		if err == nil {
			return nil
		}
		lastErr = err
		if !isTransient(err) {
			return err
		}
	}
	if lastErr == nil {
		lastErr = errNoMaster
	}
	return lastErr
}

// Put uploads the file at localPath and stores it at remotePath. The file is
// split into chunks, each written to its planned chunk servers, and the file is
// finalised in the namespace once all chunks are durable.
func (c *Client) Put(ctx context.Context, localPath, remotePath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("client: open %s: %w", localPath, err)
	}
	defer func() { _ = f.Close() }()

	chunks, err := splitChunks(f, c.chunkSize)
	if err != nil {
		return err
	}

	chunkIDs := make([]string, len(chunks))
	var total int64
	for i, ch := range chunks {
		chunkIDs[i] = ch.id
		total += int64(len(ch.data))
	}

	var plan *vaultfsv1.OpenForWriteResponse
	if err := c.callMaster(ctx, func(m vaultfsv1.MasterServiceClient) error {
		var err error
		plan, err = m.OpenForWrite(ctx, &vaultfsv1.OpenForWriteRequest{
			Path: remotePath, ChunkIds: chunkIDs,
		})
		return err
	}); err != nil {
		return fmt.Errorf("client: open for write %s: %w", remotePath, err)
	}
	if len(plan.GetPlacements()) != len(chunks) {
		return fmt.Errorf("client: master returned %d placements for %d chunks",
			len(plan.GetPlacements()), len(chunks))
	}

	for i, ch := range chunks {
		if err := c.writeChunk(ctx, plan.GetPlacements()[i], ch); err != nil {
			return err
		}
	}

	if err := c.callMaster(ctx, func(m vaultfsv1.MasterServiceClient) error {
		_, err := m.FinalizeWrite(ctx, &vaultfsv1.FinalizeWriteRequest{
			Path: remotePath, ChunkIds: chunkIDs, Size: total,
		})
		return err
	}); err != nil {
		return fmt.Errorf("client: finalize write %s: %w", remotePath, err)
	}
	return nil
}

// writeChunk sends one chunk to its primary chunk server, passing the secondary
// addresses as the replication chain.
func (c *Client) writeChunk(ctx context.Context, placement *vaultfsv1.ChunkLocation, ch chunk) error {
	nodes := placement.GetNodes()
	if len(nodes) == 0 {
		return fmt.Errorf("client: no chunk server assigned for chunk %s", ch.id)
	}
	primary := nodes[0]
	downstream := make([]string, 0, len(nodes)-1)
	for _, n := range nodes[1:] {
		downstream = append(downstream, n.GetAddress())
	}

	cc, err := c.pool.chunkClient(primary.GetAddress())
	if err != nil {
		return err
	}
	return c.retry.do(ctx, func() error {
		_, err := cc.WriteChunk(ctx, &vaultfsv1.WriteChunkRequest{
			ChunkId: ch.id, Data: ch.data, Downstream: downstream,
		})
		return err
	})
}

// Get downloads remotePath and writes it to localPath, verifying every chunk's
// integrity against its content address.
func (c *Client) Get(ctx context.Context, remotePath, localPath string) error {
	var resp *vaultfsv1.OpenForReadResponse
	if err := c.callMaster(ctx, func(m vaultfsv1.MasterServiceClient) error {
		var err error
		resp, err = m.OpenForRead(ctx, &vaultfsv1.OpenForReadRequest{Path: remotePath})
		return err
	}); err != nil {
		return fmt.Errorf("client: open for read %s: %w", remotePath, err)
	}

	out, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("client: create %s: %w", localPath, err)
	}
	defer func() { _ = out.Close() }()

	locations := resp.GetLocations()
	for i, chunkID := range resp.GetFile().GetChunkIds() {
		if i >= len(locations) {
			return fmt.Errorf("client: missing location for chunk %d (%s)", i, chunkID)
		}
		data, err := c.readChunk(ctx, chunkID, locations[i])
		if err != nil {
			return err
		}
		if _, err := out.Write(data); err != nil {
			return fmt.Errorf("client: write %s: %w", localPath, err)
		}
	}
	return nil
}

// readChunk fetches one chunk, trying each replica in turn and verifying the
// bytes against the chunk ID before returning them.
func (c *Client) readChunk(ctx context.Context, chunkID string, loc *vaultfsv1.ChunkLocation) ([]byte, error) {
	var lastErr error
	for _, node := range loc.GetNodes() {
		cc, err := c.pool.chunkClient(node.GetAddress())
		if err != nil {
			lastErr = err
			continue
		}
		var resp *vaultfsv1.ReadChunkResponse
		err = c.retry.do(ctx, func() error {
			resp, err = cc.ReadChunk(ctx, &vaultfsv1.ReadChunkRequest{ChunkId: chunkID})
			return err
		})
		if err != nil {
			lastErr = err
			continue
		}
		if err := verifyChunk(chunkID, resp.GetData()); err != nil {
			lastErr = err
			continue // a corrupt replica should not fail the read if another is good
		}
		return resp.GetData(), nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("client: chunk %s has no replicas", chunkID)
	}
	return nil, fmt.Errorf("client: read chunk %s: %w", chunkID, lastErr)
}

// Stat returns the metadata for path.
func (c *Client) Stat(ctx context.Context, path string) (*vaultfsv1.FileInfo, error) {
	var resp *vaultfsv1.StatResponse
	if err := c.callMaster(ctx, func(m vaultfsv1.MasterServiceClient) error {
		var err error
		resp, err = m.Stat(ctx, &vaultfsv1.StatRequest{Path: path})
		return err
	}); err != nil {
		return nil, fmt.Errorf("client: stat %s: %w", path, err)
	}
	return resp.GetFile(), nil
}

// ListDir returns the direct children of the directory at path.
func (c *Client) ListDir(ctx context.Context, path string) ([]*vaultfsv1.FileInfo, error) {
	var resp *vaultfsv1.ListDirResponse
	if err := c.callMaster(ctx, func(m vaultfsv1.MasterServiceClient) error {
		var err error
		resp, err = m.ListDir(ctx, &vaultfsv1.ListDirRequest{Path: path})
		return err
	}); err != nil {
		return nil, fmt.Errorf("client: list %s: %w", path, err)
	}
	return resp.GetEntries(), nil
}

// Delete removes the file at path from the namespace.
func (c *Client) Delete(ctx context.Context, path string) error {
	if err := c.callMaster(ctx, func(m vaultfsv1.MasterServiceClient) error {
		_, err := m.DeleteFile(ctx, &vaultfsv1.DeleteFileRequest{Path: path})
		return err
	}); err != nil {
		return fmt.Errorf("client: delete %s: %w", path, err)
	}
	return nil
}

// Status returns a snapshot of cluster health from the admin service.
func (c *Client) Status(ctx context.Context) (*vaultfsv1.ClusterStatusResponse, error) {
	var resp *vaultfsv1.ClusterStatusResponse
	if err := c.callAdmin(ctx, func(a vaultfsv1.AdminServiceClient) error {
		var err error
		resp, err = a.ClusterStatus(ctx, &vaultfsv1.ClusterStatusRequest{})
		return err
	}); err != nil {
		return nil, fmt.Errorf("client: cluster status: %w", err)
	}
	return resp, nil
}
