#!/usr/bin/perl


while(<>) {
  s/^rem/-- /;

  # replace "customer" with customer
  s/"(\w+)"/$1/g;

  # replace to_date('1991-9-10','YYYY-MM-DD') with '1992-9-10'
  s/to_date.('\d+-\d+-\d+'),'YYYY-MM-DD'./$1/g;
  print;
}
