module github.com/LiveRamp/factable

go 1.12

require (
	github.com/cockroachdb/cockroach v1.0.0
	github.com/coreos/bbolt v1.3.2 // indirect
	github.com/coreos/etcd v3.3.12+incompatible // indirect
	github.com/coreos/go-systemd v0.0.0-20181031085051-9002847aa142 // indirect
	github.com/coreos/pkg v0.0.0-20180108230652-97fdf19511ea // indirect
	github.com/dustinkirkland/golang-petname v0.0.0-20190613200456-11339a705ed2
	github.com/go-check/check v0.0.0-20180628173108-788fd7840127
	github.com/gogo/protobuf v1.3.1
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/golang/protobuf v1.3.2
	github.com/google/uuid v1.1.1
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.8.1 // indirect
	github.com/jessevdk/go-flags v1.4.1-0.20181221193153-c0795c8afcf4
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v0.9.3-0.20190127221311-3c4408c8b829
	github.com/prometheus/client_model v0.0.0-20190129233127-fd36f4220a90 // indirect
	github.com/prometheus/procfs v0.0.0-20190227231451-bbced9601137 // indirect
	github.com/retailnext/hllpp v0.0.0-20170130195621-01496c854800
	github.com/sirupsen/logrus v1.3.0
	github.com/tecbot/gorocksdb v0.0.0-20190705090504-162552197222
	// github.com/tecbot/gorocksdb v0.0.0-20190705090504-162552197222
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/ugorji/go v1.1.1 // indirect
	go.etcd.io/etcd v0.0.0-20190711162406-e56e8471ec18
	go.gazette.dev/core v0.85.2
	google.golang.org/grpc v1.22.0
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.2.2
)

replace (
	github.com/cockroachdb/cockroach => github.com/jgraettinger/cockroach-encoding v1.0.0
	github.com/retailnext/hllpp => github.com/LiveRamp/hllpp v0.0.0-20170131005621-01496c854800
	github.com/tecbot/gorocksdb => github.com/LiveRamp/gorocksdb v1.2.0
)
