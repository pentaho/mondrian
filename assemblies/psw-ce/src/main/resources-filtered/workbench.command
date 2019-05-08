# ***************************************************************************
# This software is subject to the terms of the Eclipse Public License v1.0
# Agreement, available at the following URL:
# http://www.eclipse.org/legal/epl-v10.html.
# You must accept the terms of that agreement to use this software.
#
# Copyright (c) 2007 - ${copyright.year} Hitachi Vantara. All rights reserved.
# ***************************************************************************

cd `dirname $0`

# if a BASE_DIR argument has been passed to this .command, use it
if [ -n "$1" ] && [ -d "$1" ] && [ -x "$1" ]; then
    echo "DEBUG: Using value ($1) from calling script"
    cd "$1"
fi

./workbench.sh
exit