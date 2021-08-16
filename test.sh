#!/bin/sh

rm -rf /tmp/filou-test

PATH=$PWD/../_build/default/filou/:$PATH \
  dune exec test/test.exe -- --filou main.exe small | tee test/output.txt
