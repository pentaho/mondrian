#!/bin/bash

# ***************************************************************************
# This software is subject to the terms of the Eclipse Public License v1.0
# Agreement, available at the following URL:
# http://www.eclipse.org/legal/epl-v10.html.
# You must accept the terms of that agreement to use this software.
#
# Copyright (c) 2009-2020 Hitachi Vantara. All rights reserved.
# ***************************************************************************

echo "***************************************************************************"
echo "Mondrian is transitioning to checkstyle for formatting."
echo "When committing changes to files, first scan with checkstyle. (https://checkstyle.sourceforge.io/)"
echo "Leave untouched files as they are to preserve the usefulness of git-blame."
echo "Checkstyle rule file: "
echo " https://raw.githubusercontent.com/pentaho/pentaho-coding-standards/master/checkstyle/pentaho_checks.xml"
echo "***************************************************************************"
