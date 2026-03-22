#!/bin/bash

set -ev

wget https://download.microsoft.com/download/4/D/C/4DCD85FA-0041-4D2E-8DD9-833C1873978C/sqljdbc_7.2.2.0_enu.tar.gz
tar xzf sqljdbc_7.2.2.0_enu.tar.gz
cp sqljdbc_7.2/enu/mssql-jdbc-7.2.2.jre8.jar test/support/jars
