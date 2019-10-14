cmds:
	docker build --target factable --tag liveramp/factable .

base_build: git_submodules
	docker build --target build --tag liveramp/factable-base .

test: base_build
	docker run --rm liveramp/factable-base go test -tags ./pkg/...

git_submodules: .gitmodules
	git submodule update --init --recursive
