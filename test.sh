#!/bin/sh

rm -rf /tmp/filou-test
PATH=~/keep/perso/dunable/_build/default/filou69/:$PATH \
  dune exec test/test.exe -- --filou main.exe | tee test/output.txt
