factable:
	docker build --target factable --tag liveramp/factable .

build: git_submodules
	docker build --target build --tag liveramp/factable-base .

clean:
	docker image rm -f liveramp/factable-base
	docker image rm -f liveramp/factable

test: build
	docker run --rm liveramp/factable-base go test -tags rocksdb ./pkg/...

git_submodules: .gitmodules
	git submodule update --init --recursive
