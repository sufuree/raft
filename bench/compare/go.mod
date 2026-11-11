module github.com/sufuree/raft/bench/compare

go 1.27

require (
	github.com/hashicorp/raft v1.8.0
	github.com/hashicorp/raft-boltdb/v2 v2.4.2
	github.com/sufuree/raft v0.0.0
	go.etcd.io/raft/v3 v3.7.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/hashicorp/go-hclog v1.6.3 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-metrics v0.7.0 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.5 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	go.etcd.io/bbolt v1.4.1 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/sufuree/raft => ../..
