package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

// gobCodec is a gRPC codec backed by encoding/gob. It lets us send typed Go
// structs over gRPC without protobuf code generation.
type gobCodec struct{}

func (gobCodec) Marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, fmt.Errorf("raft: gob marshal: %w", err)
	}
	return buf.Bytes(), nil
}

func (gobCodec) Unmarshal(data []byte, v interface{}) error {
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(v); err != nil {
		return fmt.Errorf("raft: gob unmarshal: %w", err)
	}
	return nil
}

func (gobCodec) Name() string { return "gob" }

// raftServiceDesc describes the Raft gRPC service without a .proto file.
// Handler functions dispatch to the registered RPCHandler.
var raftServiceDesc = grpc.ServiceDesc{
	ServiceName: "raft.v1.RaftService",
	HandlerType: (*RPCHandler)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RequestVote",
			Handler:    handleRequestVoteGRPC,
		},
		{
			MethodName: "AppendEntries",
			Handler:    handleAppendEntriesGRPC,
		},
		{
			MethodName: "InstallSnapshot",
			Handler:    handleInstallSnapshotGRPC,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "raft.v1.proto",
}

func handleRequestVoteGRPC(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RequestVoteArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	h := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RPCHandler).HandleRequestVote(ctx, *req.(*RequestVoteArgs))
	}
	if interceptor == nil {
		return h(ctx, in)
	}
	return interceptor(ctx, in, &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/raft.v1.RaftService/RequestVote",
	}, h)
}

func handleAppendEntriesGRPC(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AppendEntriesArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	h := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RPCHandler).HandleAppendEntries(ctx, *req.(*AppendEntriesArgs))
	}
	if interceptor == nil {
		return h(ctx, in)
	}
	return interceptor(ctx, in, &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/raft.v1.RaftService/AppendEntries",
	}, h)
}

func handleInstallSnapshotGRPC(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InstallSnapshotArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	h := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RPCHandler).HandleInstallSnapshot(ctx, *req.(*InstallSnapshotArgs))
	}
	if interceptor == nil {
		return h(ctx, in)
	}
	return interceptor(ctx, in, &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/raft.v1.RaftService/InstallSnapshot",
	}, h)
}

// GRPCTransport is a production Transport that sends Raft RPCs over gRPC using
// gob encoding. Use NewGRPCTransport to create one.
type GRPCTransport struct {
	mu         sync.RWMutex // protects conns
	id         string
	server     *grpc.Server
	conns      map[string]*grpc.ClientConn // peer addr -> connection
	handler    RPCHandler
	clientCred credentials.TransportCredentials
}

// GRPCOption configures a GRPCTransport.
type GRPCOption func(*grpcOptions)

type grpcOptions struct {
	serverCred credentials.TransportCredentials
	clientCred credentials.TransportCredentials
}

// WithTLS secures the Raft transport with mutual TLS: serverCred authenticates
// inbound peers, clientCred authenticates this node when dialing peers.
func WithTLS(serverCred, clientCred credentials.TransportCredentials) GRPCOption {
	return func(o *grpcOptions) {
		o.serverCred = serverCred
		o.clientCred = clientCred
	}
}

// NewGRPCTransport creates a GRPCTransport that listens on addr.
// Connections to peers are established lazily on first use. Without options the
// transport is insecure; pass WithTLS to enable mutual TLS.
func NewGRPCTransport(addr string, opts ...GRPCOption) (*GRPCTransport, error) {
	// Register gob codec once so both client and server use it.
	encoding.RegisterCodec(gobCodec{})

	var o grpcOptions
	for _, opt := range opts {
		opt(&o)
	}

	var srvOpts []grpc.ServerOption
	if o.serverCred != nil {
		srvOpts = append(srvOpts, grpc.Creds(o.serverCred))
	}
	srv := grpc.NewServer(srvOpts...)
	t := &GRPCTransport{
		server:     srv,
		conns:      make(map[string]*grpc.ClientConn),
		clientCred: o.clientCred,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("raft: listen on %s: %w", addr, err)
	}
	go func() {
		if err := srv.Serve(ln); err != nil {
			slog.Error("raft: grpc server stopped", "err", err)
		}
	}()
	return t, nil
}

// Register wires the given RPCHandler into the gRPC server.
func (t *GRPCTransport) Register(id string, handler RPCHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.id = id
	t.handler = handler
	t.server.RegisterService(&raftServiceDesc, handler)
}

// Close stops the gRPC server and all client connections.
func (t *GRPCTransport) Close() error {
	t.server.GracefulStop()
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		_ = c.Close()
	}
	return nil
}

func (t *GRPCTransport) conn(peer string) (*grpc.ClientConn, error) {
	t.mu.RLock()
	c, ok := t.conns[peer]
	t.mu.RUnlock()
	if ok {
		return c, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok = t.conns[peer]; ok {
		return c, nil
	}
	cred := t.clientCred
	if cred == nil {
		cred = insecure.NewCredentials()
	}
	c, err := grpc.NewClient(peer, grpc.WithTransportCredentials(cred))
	if err != nil {
		return nil, fmt.Errorf("raft: dial %s: %w", peer, err)
	}
	t.conns[peer] = c
	return c, nil
}

func (t *GRPCTransport) SendRequestVote(ctx context.Context, peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	c, err := t.conn(peer)
	if err != nil {
		return RequestVoteReply{}, err
	}
	var reply RequestVoteReply
	err = c.Invoke(ctx, "/raft.v1.RaftService/RequestVote", &args, &reply,
		grpc.ForceCodec(gobCodec{}))
	return reply, err
}

func (t *GRPCTransport) SendAppendEntries(ctx context.Context, peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	c, err := t.conn(peer)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	var reply AppendEntriesReply
	err = c.Invoke(ctx, "/raft.v1.RaftService/AppendEntries", &args, &reply,
		grpc.ForceCodec(gobCodec{}))
	return reply, err
}

func (t *GRPCTransport) SendInstallSnapshot(ctx context.Context, peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	c, err := t.conn(peer)
	if err != nil {
		return InstallSnapshotReply{}, err
	}
	var reply InstallSnapshotReply
	err = c.Invoke(ctx, "/raft.v1.RaftService/InstallSnapshot", &args, &reply,
		grpc.ForceCodec(gobCodec{}))
	return reply, err
}
