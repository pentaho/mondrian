#!/usr/bin/perl

while(<>) {
  s/^rem/-- /;
  s/"/ /g;
  print;
}
