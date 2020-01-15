factable:
	docker build --target factable --tag liveramp/factable .
#
#gazette-ci-builder:
#	git clone https://github.com/gazette/core.git /tmp/gazette
#	cd /tmp/gazette
#	git checkout v0.85.2
#	make
#
#	docker build
build: git_submodules
	docker build --target build --tag liveramp/factable-base .

clean:
	docker image rm -f liveramp/factable-base
	docker image rm -f liveramp/factable

test: build
	docker run --rm liveramp/factable-base go test -tags rocksdb ./pkg/...

git_submodules: .gitmodules
	git submodule update --init --recursive
