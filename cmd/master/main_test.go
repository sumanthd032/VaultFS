package main

import (
	"reflect"
	"testing"
)

func TestDropSelf(t *testing.T) {
	tests := []struct {
		name   string
		peers  []string
		nodeID string
		want   []string
	}{
		{
			name:   "compose peers already exclude self",
			peers:  []string{"master-1:9200", "master-2:9200"},
			nodeID: "master-0",
			want:   []string{"master-1:9200", "master-2:9200"},
		},
		{
			name:   "statefulset fqdn peers include self",
			peers:  []string{"vaultfs-master-0.vaultfs-master:9200", "vaultfs-master-1.vaultfs-master:9200"},
			nodeID: "vaultfs-master-0",
			want:   []string{"vaultfs-master-1.vaultfs-master:9200"},
		},
		{
			name:   "self with full cluster domain",
			peers:  []string{"vaultfs-master-2.vaultfs-master.vaultfs.svc.cluster.local:9200"},
			nodeID: "vaultfs-master-2",
			want:   nil,
		},
		{
			name:   "no port",
			peers:  []string{"master-0", "master-1"},
			nodeID: "master-0",
			want:   []string{"master-1"},
		},
		{
			name:   "empty list",
			peers:  nil,
			nodeID: "master-0",
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dropSelf(append([]string(nil), tt.peers...), tt.nodeID)
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("dropSelf(%v, %q) = %v, want %v", tt.peers, tt.nodeID, got, tt.want)
			}
		})
	}
}

func TestEnvOrInt(t *testing.T) {
	tests := []struct {
		name string
		set  string
		def  int
		want int
	}{
		{name: "unset returns default", set: "", def: 3, want: 3},
		{name: "valid integer", set: "5", def: 3, want: 5},
		{name: "unparsable returns default", set: "abc", def: 3, want: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const key = "VAULTFS_TEST_REPLICATION"
			if tt.set != "" {
				t.Setenv(key, tt.set)
			}
			if got := envOrInt(key, tt.def); got != tt.want {
				t.Fatalf("envOrInt(%q, %d) = %d, want %d", tt.set, tt.def, got, tt.want)
			}
		})
	}
}
