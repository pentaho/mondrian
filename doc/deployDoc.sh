#!/usr/bin/sh
# $Id$
# This software is subject to the terms of the Common Public License
# Agreement, available at the following URL:
# http://www.opensource.org/licenses/cpl.html.
# Copyright (C) 2005-2005 Julian Hyde
# All Rights Reserved.
# You must accept the terms of that agreement to use this software.
#
# This is a script to deploy Mondrian's website.
# Only the release manager (jhyde) should run this script.

set -e
set -v

cd $(dirname $0)/..
ant doczip

scp dist/doc.tar.gz jhyde@shell.sf.net:

GROUP_DIR=/home/groups/m/mo/mondrian

ssh -T jhyde@shell.sf.net <<EOF
set -e
set -v
rm -f $GROUP_DIR/doc.tar.gz
mv doc.tar.gz $GROUP_DIR
cd $GROUP_DIR
tar xzf doc.tar.gz
rm -rf old
mv htdocs old
mv doc htdocs
rm -rf old
EOF

# End deployDoc.sh
