#!/usr/bin/perl

while(<>) {
  s/^rem/-- /;
  s/integer/INTEGER/g;
  s/varchar/VARCHAR/g;
  s/ date/ DATE/g;
  s/"/ /g;
  print;
}
