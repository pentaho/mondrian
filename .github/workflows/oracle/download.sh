#!/usr/bin/env bash

cd "$(dirname "$(readlink -f "$0")")"

deb_file=oracle-xe_11.2.0-1.0_amd64.deb

git clone https://github.com/wnameless/docker-oracle-xe-11g.git

cd docker-oracle-xe-11g/assets &&
      cat "${deb_file}aa" "${deb_file}ab" "${deb_file}ac" > "${deb_file}"

pwd

ls -lAh "${deb_file}"
