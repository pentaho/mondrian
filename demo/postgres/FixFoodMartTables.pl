#!/usr/bin/perl

while(<>) {
  s/^rem/-- /;
  s/integer/BIGINT/g;
  s/varchar/VARCHAR/g;
  s/ date/ DATE/g;
  s/"/ /g;
  print;
}
