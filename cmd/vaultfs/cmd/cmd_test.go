package cmd

import (
	"bytes"
	"testing"

	vaultfsv1 "github.com/sumanthd032/vaultfs/proto/vaultfs/v1"
)

func TestRootCommandWiring(t *testing.T) {
	root := NewRootCommand()

	want := map[string]bool{"put": false, "get": false, "ls": false, "rm": false, "status": false}
	for _, c := range root.Commands() {
		want[c.Name()] = true
	}
	for name, found := range want {
		if !found {
			t.Errorf("subcommand %q not registered", name)
		}
	}
}

func TestPersistentFlagDefaults(t *testing.T) {
	root := NewRootCommand()
	if f := root.PersistentFlags().Lookup("masters"); f == nil {
		t.Fatal("--masters flag missing")
	}
	if f := root.PersistentFlags().Lookup("timeout"); f == nil {
		t.Fatal("--timeout flag missing")
	}
}

func TestArgValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{"put needs two args", []string{"put", "only-one"}, true},
		{"get needs two args", []string{"get", "only-one"}, true},
		{"ls needs one arg", []string{"ls"}, true},
		{"rm needs one arg", []string{"rm"}, true},
		{"status takes no args", []string{"status", "extra"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := NewRootCommand()
			root.SetOut(&bytes.Buffer{})
			root.SetErr(&bytes.Buffer{})
			root.SetArgs(tt.args)
			err := root.Execute()
			if tt.wantErr && err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

func TestFormatSize(t *testing.T) {
	tests := []struct {
		in   int64
		want string
	}{
		{0, "0B"},
		{512, "512B"},
		{1024, "1.0KB"},
		{1536, "1.5KB"},
		{1048576, "1.0MB"},
	}
	for _, tt := range tests {
		if got := formatSize(tt.in); got != tt.want {
			t.Errorf("formatSize(%d) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestNodeStateString(t *testing.T) {
	tests := []struct {
		in   vaultfsv1.NodeState
		want string
	}{
		{vaultfsv1.NodeState_NODE_STATE_ALIVE, "alive"},
		{vaultfsv1.NodeState_NODE_STATE_DEAD, "dead"},
		{vaultfsv1.NodeState_NODE_STATE_UNSPECIFIED, "unknown"},
	}
	for _, tt := range tests {
		if got := nodeStateString(tt.in); got != tt.want {
			t.Errorf("nodeStateString(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestHeartbeatAgeNever(t *testing.T) {
	if got := heartbeatAge(0); got != "never" {
		t.Errorf("heartbeatAge(0) = %q, want never", got)
	}
}
