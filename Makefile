.PHONY: default
default:
	dune build

.PHONY: test
test: test-small test-large

.PHONY: test-ssh
test-ssh: test-ssh-small test-ssh-large

.PHONY: test-full
test-full: test test-ssh

test-%: default
	rm -rf /tmp/filou-test
	mkdir -p test/output
	PATH=$(PWD)/../_build/default/filou/:$(PATH) \
	  dune exec test/test.exe -- --filou main.exe $* | tee test/output/$*.txt

test-ssh-%: default
	rm -rf /tmp/filou-test
	mkdir -p test/output
	PATH=$(PWD)/../_build/default/filou/:$(PATH) \
	  dune exec test/test.exe -- --filou main.exe --ssh $* | tee test/output/$*-ssh.txt
