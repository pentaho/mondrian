#!/usr/bin/env bash

[ -n "$ORACLE_FILE" ] || { echo "Missing ORACLE_FILE environment variable!"; exit 1; }
[ -n "$ORACLE_HOME" ] || { echo "Missing ORACLE_HOME environment variable!"; exit 1; }

cd "$(dirname "$(readlink -f "$0")")"

ORACLE_DEB=docker-oracle-xe-11g/assets/oracle-xe_11.2.0-1.0_amd64.deb

sudo apt-get -qq update
sudo apt-get --no-install-recommends -qq install bc libaio1

# Add the symlink for libaio to fix "E: Package 'libaio1' has no installation candidate" on ubuntu 24.04
sudo ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1

df -B1 /dev/shm | awk 'END { if ($1 != "shmfs" && $1 != "tmpfs" || $2 < 2147483648) exit 1 }' ||
    ( sudo rm -r /dev/shm && sudo mkdir /dev/shm && sudo mount -t tmpfs shmfs -o size=2G /dev/shm )

test -f /sbin/chkconfig ||
    ( echo '#!/bin/sh' | sudo tee /sbin/chkconfig > /dev/null && sudo chmod u+x /sbin/chkconfig )

test -d /var/lock/subsys || sudo mkdir /var/lock/subsys

sudo dpkg -i "${ORACLE_DEB}"

echo 'OS_AUTHENT_PREFIX=""' | sudo tee -a "$ORACLE_HOME/config/scripts/init.ora" > /dev/null
echo 'disk_asynch_io=false' | sudo tee -a "$ORACLE_HOME/config/scripts/init.ora" > /dev/null
sudo usermod -aG dba $USER

( echo ; echo ; echo github ; echo github ; echo n ) | sudo AWK='/usr/bin/awk' /etc/init.d/oracle-xe configure

"$ORACLE_HOME/bin/sqlplus" -L -S / AS SYSDBA <<SQL
CREATE USER $USER IDENTIFIED EXTERNALLY;
GRANT CONNECT, RESOURCE TO $USER;
ALTER SYSTEM SET PARALLEL_MAX_SERVERS = 36 SCOPE=BOTH;
ALTER SYSTEM SET OPEN_CURSORS = 3000 SCOPE=BOTH;
SQL
