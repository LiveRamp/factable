factable:
	docker build --target factable --tag liveramp/factable .

base_build: git_submodules
	docker build --target build --tag liveramp/factable-base .

clean:
	docker image rm liveramp/factable-base
	docker image rm liveramp/factable

test: base_build
	docker run --rm liveramp/factable-base go test -tags rocksdb ./pkg/...

git_submodules: .gitmodules
	git submodule update --init --recursive
