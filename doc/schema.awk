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
function error(msg) {
    printf "Error: %s, at line %d:\n%s", msg, FNR, $0;
    exit;
}

function startsWith(s, r) {
    return substr(s, 1, length(r)) == r;
}

function indexFrom(s, r, i) {
    while (i <= length(s)) {
        if (substr(s, i, length(r)) == r) {
            return i;
        }
        ++i;
    }
    return 0;
}

function indexFromAfter(s, r, i) {
    return indexFrom(s, r, i) + length(r);
}

function splice(s, start, mid, end) {
    return substr(s, 1, start - 1)              \
        mid                                     \
        substr(s, end);
}

function assertEquals(x, y) {
    assert(x == y, x " == " y);
}

function assert(b, msg) {
    if (!b) {
        print "error: " msg;
        exit;
    }
}

function elementize(s, \
                    o, i, element) {
    if (startsWith(s, "&lt;/")) {
        prefix = "&lt;/";
    } else {
        prefix = "&lt;";
    }
    o = length(prefix) + 1;
    for (i = o; index(letters, substr(s, i, 1)) > 0; i++) {}
    element = substr(s, o, i - o);
    return sprintf("%s<a href='#XML_%s'>%s</a>%s", \
                   substr(s, 1, o - 1), element, element, substr(s, i));
}

function elementTag(s,      i, j, k, m, name) {
    i = index(s, "{@element ");
    if (i == 0) {
        return s;
    }
    j = i + length("{@element ");
    for (k = j; index(letters, substr(s, k, 1)); k++) {}
    for (n = k; substr(s, n, 1) != "}"; n++) {}
    name = substr(s, j, k - j);
    rest = substr(s, k, n - k);
    return substr(s, 1, i - 1)                                          \
        sprintf("<code>&lt;<a href='#XML_%s'>%s</a>%s&gt;</code>",
                name, name, rest)               \
        substr(s, n + 1);
}

BEGIN {
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_";

    # sanity checks
    assertEquals(splice("abcdefgh", 4, "wxyz", 6), "abcwxyzfgh");
    assertEquals(1, indexFrom("abcdeabc", "abc", 1));
    assertEquals(6, indexFrom("abcdeabc", "abc", 2));
    assertEquals(0, indexFrom("abcdeabc", "abcd", 2));
    assertEquals(1, startsWith("abcd", "ab"));
    assertEquals(0, startsWith("abcd", "abd"));
    assertEquals(0, startsWith("abcd", "bc"));
    assertEquals("&lt;<a href='#XML_Abc'>Abc</a>&gt;",
                 elementize("&lt;Abc&gt;"));
    assertEquals("&lt;/<a href='#XML_Abc'>Abc</a>&gt;",
                 elementize("&lt;/Abc&gt;"));
    assertEquals("&lt;<a href='#XML_Abc'>Abc</a> x=y/&gt;",
                 elementize("&lt;Abc x=y/&gt;"));
    assertEquals("&lt;<a href='#XML_Abc'>Abc</a> x=y&gt;",
                 elementize("&lt;Abc x=y&gt;"));
    assertEquals("<code>&lt;<a href='#XML_Abc'>Abc</a>&gt;</code>",
                 elementTag("{@element Abc}"));
    assertEquals("<code>&lt;<a href='#XML_Abc'>Abc</a> x=y&gt;</code>",
                 elementTag("{@element Abc x=y}"));
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
    printf "    %s<div style='padding-left:%dpx;'>", spaces, length(spaces) * 10 + 20;
    gsub(/</, "\\&lt;", s);
    gsub(/>/, "\\&gt;", s);
    gsub(/{@ignore .*}/, "", s);
    i = 0;
    for (;;) {
        i = indexFrom(s, "&lt;", i + 1);
        if (i == 0) {
            break;
        }
        j = indexFromAfter(s, "&gt;", i);
        element = substr(s, i, j - i);
        x = elementize(element);
        s = splice(s, i, x, j);
        i = i + length(x) - length(element);
    }
    printf "%s</div>\n", s;
    next;
}
/{@element *$/ {
    error("Incomplete element tag");
}
/{@element/ {
    if ($0 !~ /{@element .*}/) {
        error("Incomplete element tag");
    }
    for (;;) {
        prev = s;
        s = elementTag(s);
        if (s == prev) {
            break;
        }
    }
}
/{@xml/ {
    inXml = 1;
    printf "<blockquote>\n";
    printf "  <code style='text-indent: -20px'>\n";
    next;
}
{
    print s;
}

# End schema.awk
