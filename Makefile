.PHONY: default
default:
	dune build

.PHONY: test
test: default
	./test.sh
