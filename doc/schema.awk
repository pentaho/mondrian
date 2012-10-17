#!/bin/gawk
#
# This software is subject to the terms of the Eclipse Public License v1.0
# Agreement, available at the following URL:
# http://www.eclipse.org/legal/epl-v10.html.
# You must accept the terms of that agreement to use this software.
#
# Copyright (C) 2012-2012 Pentaho
# All Rights Reserved.
#
# Generates schema.html from schema.html.template, replacing occurrences
# of
#
#  {@xml
#    <An xml=element/>
#  }
#
# and inline {@element AnXmlElement} references.
#
BEGIN {
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_";
}
{
    s = $0;
}
inXml && /^}/ {
    printf "  </code>\n";
    printf "</blockquote>";
    inXml = 0;
    next;
}
inXml {
    for (i = 1; substr(s, i, 1) == " "; i++) {}
    spaces = substr(s, 1, i - 1);
    s = substr(s, i);
    printf "    %s<div style='padding-left:%dpx;'>", spaces, length(spaces) * 10;
    gsub(/</, "\\&lt;", s);
    gsub(/>/, "\\&gt;", s);
    if (s ~ /&lt;\//) {
        o = 6;
        for (i = o; index(letters, substr(s, i, 1)) > 0; i++) {}
        element = substr(s, o, i - o);
        printf "%s<a href='#XML_%s'>%s</a>%s", substr(s, 1, o - 1), element, element, substr(s, i);
    } else if (s ~ /&lt;/) {
        o = 5;
        for (i = o; index(letters, substr(s, i, 1)) > 0; i++) {}
        element = substr(s, o, i - o);
        printf "%s<a href='#XML_%s'>%s</a>%s", substr(s, 1, o - 1), element, element, substr(s, i);
    } else {
        printf "%s", s;
    }
    printf "</div>\n";
    next;
}
/{@element / {
    i = index(s, "{@element ");
    j = i + length("{@element ");
#    printf "xxx[%s]\n", substr(s, i);
#    printf "xxx[%s]\n", substr(s, j);
    for (k = j; substr(s, k, 1) != "}"; k++) {}
    name = substr(s, j, k - j);
    s = substr(s, 1, i - 1)                                             \
        sprintf("<code>&lt;<a href='#XML_%s'>%s</a>&gt;</code>", name, name) \
        substr(s, k + 1);
}
/{@xml/ {
    inXml = 1;
    printf "<blockquote>\n";
    printf "  <code style='text-indent: 20px'>\n";
    next;
}
{
    print s;
}

// End schema.awk
