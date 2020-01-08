module github.com/LiveRamp/factable

go 1.12

require (
	cloud.google.com/go v0.43.0 // indirect
	github.com/DataDog/zstd v1.4.0 // indirect
	github.com/LiveRamp/gazette v0.0.0-20190703144706-49c4ae6934ca
	github.com/aws/aws-sdk-go v1.19.32 // indirect
	github.com/cockroachdb/cockroach v1.0.0
	github.com/coreos/bbolt v1.3.2 // indirect
	github.com/coreos/etcd v3.3.12+incompatible
	github.com/coreos/go-semver v0.2.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20181031085051-9002847aa142 // indirect
	github.com/coreos/pkg v0.0.0-20180108230652-97fdf19511ea // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/dustinkirkland/golang-petname v0.0.0-20170921220637-d3c2ba80e75e
	github.com/facebookgo/ensure v0.0.0-20160127193407-b4ab57deab51 // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20150612182917-8dac2c3c4870 // indirect
	github.com/go-check/check v0.0.0-20180628173108-788fd7840127
	github.com/gogo/protobuf v1.2.1
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/golang/protobuf v1.3.2
	github.com/golang/snappy v0.0.1 // indirect
	github.com/gorilla/schema v1.1.0 // indirect
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.8.1 // indirect
	github.com/jessevdk/go-flags v1.4.1-0.20181221193153-c0795c8afcf4
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/klauspost/compress v1.5.0 // indirect
	github.com/klauspost/cpuid v1.2.1 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v0.9.3-0.20190127221311-3c4408c8b829
	github.com/prometheus/client_model v0.0.0-20190129233127-fd36f4220a90 // indirect
	github.com/prometheus/procfs v0.0.0-20190227231451-bbced9601137 // indirect
	github.com/retailnext/hllpp v0.0.0-20170130195621-01496c854800
	github.com/sirupsen/logrus v1.3.0
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/spf13/afero v1.2.2 // indirect
	github.com/stretchr/testify v1.3.0 // indirect
	github.com/tecbot/gorocksdb v1.2.0
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/ugorji/go v1.1.1 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.3 // indirect
	go.uber.org/atomic v1.3.2 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.9.1 // indirect
	golang.org/x/crypto v0.0.0-20190701094942-4def268fd1a4 // indirect
	golang.org/x/net v0.0.0-20190724013045-ca1201d0de80
	golang.org/x/sys v0.0.0-20190712062909-fae7ac547cb7 // indirect
	google.golang.org/grpc v1.22.0
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.2.2
)

replace (
	github.com/cockroachdb/cockroach => github.com/jgraettinger/cockroach-encoding v1.0.0
	github.com/retailnext/hllpp => github.com/LiveRamp/hllpp v0.0.0-20170131005621-01496c854800
	github.com/tecbot/gorocksdb => github.com/LiveRamp/gorocksdb v1.2.0
)
