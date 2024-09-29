# ******************************************************************************
#
# Pentaho
#
# Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
#
# Use of this software is governed by the Business Source License included
# in the LICENSE.TXT file.
#
# Change Date: 2028-08-13
# ******************************************************************************


set -e
set -v

generate=true
if [ "$1" == --nogen ]; then
  shift
  generate=
fi

# Prefix is usually "release" or "head"
prefix="$1"
# Directory at sf.net
docdir=
case "$prefix" in
3.0|release) export docdir=htdocs;;
head) export docdir=head;;
*) echo "Bad prefix '$prefix'"; exit 1;;
esac

cd $(dirname $0)/..
if [ "$generate" ]; then
  ant doczip
else
  echo Skipping generation...
fi

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
if [ -d $docdir ]; then mv $docdir old; fi
mv doc $docdir
rm -rf old
rm -f doc.tar.gz
./makeLinks
EOF

# End deployDoc.sh
