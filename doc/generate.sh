#!/bin/bash
#
# This software is subject to the terms of the Eclipse Public License v1.0
# Agreement, available at the following URL:
# http://www.eclipse.org/legal/epl-v10.html.
# You must accept the terms of that agreement to use this software.
#
# Copyright (C) 2012-2012 Pentaho
# All Rights Reserved.
#
# Generates documentation (not javadoc)
#
cd $(dirname $0)
gawk -f schema.awk schema.html.template > schema.html

# End generate.sh
