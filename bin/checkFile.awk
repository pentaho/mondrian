#!/bin/gawk
# $Id: //open/util/bin/checkFile#13 $
# Checks that a file is valid.

function error(fname, linum, msg) {
    printf "%s:%d: %s\n", fname, linum, msg;
    if (0) print; # for debug
}
function _matchFile(fname) {
    return fname ~ "/mondrian/" \
       || fname ~ "/org/olap4j/" \
       || fname ~ "/aspen/" \
       || fname ~ "/farrago/" \
       || fname ~ "/fennel/" \
       || fname ~ "/extensions/" \
       || fname ~ "/com/sqlstream/" \
       || !lenient;
}
function _isCpp(fname) {
    return fname ~ /\.(cpp|h)$/;
}
function _isJava(fname) {
    return fname ~ /\.(java|jj)$/;
}
function _isMondrian(fname) {
    return fname ~ /mondrian/;
}
function push(val) {
    switchStack[switchStackLen++] = val;
}
function pop() {
    --switchStackLen
    val = switchStack[switchStackLen];
    delete switchStack[switchStackLen];
    return val;
}
function afterFile() {
    # Compute basename. If fname="/foo/bar/baz.txt" then basename="baz.txt".
    basename = fname;
    gsub(".*/", "", basename);
    gsub(lf, "", lastNonEmptyLine);
    terminator = "// End " basename;
    if (matchFile && (lastNonEmptyLine != terminator)) {
        error(fname, FNR, sprintf("Last line should be %c%s%c", 39, terminator, 39));
    }
}
# Returns whether there are unmatched open parentheses.
# unmatchedOpenParens("if ()") returns false.
# unmatchedOpenParens("if (") returns true.
# unmatchedOpenParens("if (foo) bar(") returns false
function unmatchedOpenParens(s) {
    i = index(s, "(");
    if (i == 0) {
    if (0)         print FNR, "unmatchedOpenParens=0";
        return 0;
    }
    openCount = 1;
    while (++i <= length(s)) {
        c = substr(s, i, 1);
        if (c == "(") {
            ++openCount;
        }
        if (c == ")") {
            if (--openCount == 0) {
    if (0)         print FNR, "unmatchedOpenParens=0 (b)";
                return 0;
            }
        }
    }
    if (0) print FNR, "unmatchedOpenParens=1";
    return 1;
}

function countLeadingSpaces(str) {
    i = 0;
    while (i < length(str) && substr(str, i + 1, 1) == " ") {
        ++i;
    }
    return i;
}

BEGIN {
    # pre-compute regexp for quotes, linefeed
    apos = sprintf("%c", 39);
    quot = sprintf("%c", 34);
    lf = sprintf("%c", 13);
    pattern = apos "(\\" apos "|[^" apos "])" apos;
    if (0) printf "maxLineLength=%s lenient=%s\n", maxLineLength, lenient;
}
FNR == 1 {
    if (fname) {
        afterFile();
    }
    fname = FILENAME;
    matchFile = _matchFile(fname);
    isCpp = _isCpp(fname);
    isJava = _isJava(fname);
    mondrian = _isMondrian(fname);
}
{
    if (previousLineEndedInCloseBrace > 0) {
        --previousLineEndedInCloseBrace;
    }
    if (previousLineEndedInOpenBrace > 0) {
        --previousLineEndedInOpenBrace;
    }
    if (previousLineWasEmpty > 0) {
        --previousLineWasEmpty;
    }
    s = $0;
    # remove DOS linefeeds
    gsub(lf, "", s);
    # replace strings
    gsub(/"(\\"|[^"\\]|\\[^"])*"/, "string", s);
    # replace single-quoted strings
    gsub(pattern, "string", s);
    # replace {: and :} in .cup files
    if (fname ~ /\.cup$/) {
        gsub(/{:/, "{", s);
        gsub(/:}/, "}", s);
        gsub(/:/, " : ", s);
    }
    if (inComment && $0 ~ /\*\//) {
        # end of multiline comment "*/"
        inComment = 0;
        gsub(/^.*\*\//, "/* comment */", s);
    } else if (inComment) {
        s = "/* comment */";
    } else if ($0 ~ /\/\*/ && $0 !~ /\/\*.*\*\//) {
        # beginning of multiline comment "/*"
        inComment = 1;
        gsub(/\/\*.*$/, "/* comment */", s);
    } else {
        # mask out /* */ comments
        gsub(/\/\*.*\*\//, "/* comment */", s);
    }
    if (mondrian && s ~ /\/\/\$NON-NLS/) {
        error(fname, FNR, "NON-NLS not allowed");
    }
    # mask out // comments
    gsub(/\/\/.*$/, "// comment", s);
    # line starts with string or plus?
    if (s ~ /^ *string/ \
        && s !~ /)/)
    {
        stringCol = index(s, "string");
    } else if (s ~ /^ *[+] string/) {
        if (stringCol != 0 && index(s, "+") != stringCol) {
            error(fname, FNR, "String '+' must be aligned with string on line above");
        }
    } else if (s ~ /comment/) {
        # in comment; string target carries forward
    } else {
        stringCol = 0;
    }

    # Is the line indented as expected?
    if (nextIndent > 0) {
        indent = countLeadingSpaces(s);
        if (indent != nextIndent) {
            error(fname, FNR, "Incorrect indent for first line of arg list");
        }
    }
    nextIndent = -1;
}
/ $/ {
    error(fname, FNR, "Line ends in space");
}
/[\t]/ {
    if (matchFile) {
        error(fname, FNR, "Tab character");
    }
}
/[\r]/ {
    if (matchFile) {
        error(fname, FNR, "Carriage return character (file is in DOS format?)");
    }
}
/./ {
    lastNonEmptyLine = $0;
}
{
    # Rules beyond this point only apply to Java and C++.
    if (!isCpp && !isJava) {
        next;
    }
}

/^$/ {
    if (matchFile && previousLineEndedInOpenBrace) {
        error(fname, FNR, "Empty line following open brace");
    }
}
/^ +}( catch| finally| while|[;,)])/ ||
/^ +}$/ {
    if (matchFile && previousLineWasEmpty) {
        error(fname, FNR - 1, "Empty line before close brace");
    }
}
s ~ /\<if\>.*;$/ {
    if (!matchFile) {}
    else {
        error(fname, FNR, "if followed by statement on same line");
    }
}
s ~ /\<(if) *\(/ {
    if (!matchFile) {
    } else if (s !~ /\<(if) /) {
        error(fname, FNR, "if must be followed by space");
    } else if (s ~ / else if /) {
    } else if (s ~ /^#if /) {
    } else if (s !~ /^(    )*(if)/) {
        error(fname, FNR, "if must be correctly indented");
    }
}
s ~ /\<(while) *\(/ {
    if (!matchFile) {
    } else if (s !~ /\<(while) /) {
        error(fname, FNR, "while must be followed by space");
    } else if (s ~ /} while /) {
    } else if (s !~ /^(    )+(while)/) {
        error(fname, FNR, "while must be correctly indented");
    }
}
s ~ /\<(for|switch|synchronized|} catch) *\(/ {
    if (!matchFile) {}
    else if (s !~ /^(    )*(for|switch|synchronized|} catch)/) {
        error(fname, FNR, "for/switch/synchronized/catch must be correctly indented");
    } else if (s !~ /\<(for|switch|synchronized|} catch) /) {
        error(fname, FNR, "for/switch/synchronized/catch must be followed by space");
    }
}
s ~ /\<(if|while|for|switch|catch)\>/ {
    # Check single-line if statements, such as
    #   if (condition) return;
    # We recognize such statements because there are equal numbers of open and
    # close parentheses.
    opens = s;
    gsub(/[^(]/, "", opens);
    closes = s;
    gsub(/[^)]/, "", closes);
    if (!matchFile) {
    } else if (s ~ /{( *\/\/ comment)?$/) {
        # lines which end with { and optional comment are ok
    } else if (s ~ /{.*\\$/ && isCpp) {
        # lines which end with backslash are ok in c++ macros
    } else if (s ~ /} while/) {
        # lines like "} while (foo);" are ok
    } else if (s ~ /^#/) {
        # lines like "#if 0" are ok
    } else if (s ~ /if \(true|false\)/) {
        # allow "if (true)" and "if (false)" because they are
        # used for commenting
    } else if (!unmatchedOpenParens(s)  \
               && length($0) != 79      \
               && length($0) != 80)
    {
        error(fname, FNR, "single-line if/while/for/switch/catch must end in {");
    }
}
s ~ /[[:alnum:]]\(/ &&
s !~ /\<(if|while|for|switch|assert)\>/ {
    ss = s;
    while (match(ss, /[[:alnum:]]\(/)) {
        ss = substr(ss, RSTART + RLENGTH - 1);
        parens = ss;
        gsub(/[^()]/, "", parens);
        while (substr(parens, 1, 2) == "()") {
            parens = substr(parens, 3);
        }
        opens = parens;
        gsub(/[^(]/, "", opens);
        closes = parens;
        gsub(/[^)]/, "", closes);
        if (length(opens) > length(closes)) {
            if (ss ~ /,$/) {
                bras = ss;
                gsub(/[^<]/, "", bras);
                kets = ss;
                gsub(/->/, "", kets);
                gsub(/[^>]/, "", kets);
                if (length(bras) > length(kets)) {
                    # Ignore case like 'for (Map.Entry<Foo,{nl} Bar> entry : ...'
                } else if (s ~ / for /) {
                    # Ignore case like 'for (int i = 1,{nl} j = 2; i < j; ...'
                } else {
                    error(                                              \
                        fname, FNR,                                     \
                        "multi-line parameter list should start with newline");
                    break;
                }
            } else if (s ~ /[;(]( *\\)?$/) {
                # If open paren is at end of line (with optional backslash
                # for macros), we're fine.
            } else if (s ~ /@.*\({/) {
                # Ignore Java annotations.
            } else {
                error(                                                  \
                    fname, FNR,                                         \
                    "Open parenthesis should be at end of line (function call spans several lines)");
                break;
            }
        }
        ss = substr(ss, 2); # remove initial "("
    }
}
s ~ /\<switch\>/ {
    push(switchCol);
    switchCol = index($0, "switch");
}
s ~ /{/ {
    braceCol = index($0, "{");
    if (braceCol == switchCol) {
        push(switchCol);
    }
}
s ~ /}/ {
    braceCol = index($0, "}");
    if (braceCol == switchCol) {
        switchCol = pop();
    }
}
s ~ /\<(case|default)\>/ {
    caseDefaultCol = match($0, /case|default/);
    if (!matchFile) {}
    else if (caseDefaultCol != switchCol) {
        error(fname, FNR, "case/default must be aligned with switch");
    }
}
s ~ /\<assert\>/ {
    if (!matchFile) {}
    else if (isCpp) {} # rule only applies to java
    else if (s !~ /^(    )+(assert)/) {
        error(fname, FNR, "assert must be correctly indented");
    } else if (s !~ /\<assert /) {
        error(fname, FNR, "assert must be followed by space");
    }
}
s ~ /\<return\>/ {
    if (!matchFile) {}
    else if (isCpp && s ~ /^#/) {
        # ignore macros
    } else if (s !~ /^(    )+(return)/) {
        error(fname, FNR, "return must be correctly indented");
    } else if (s !~ /\<return[ ;]/ && s !~ /\<return$/) {
        error(fname, FNR, "return must be followed by space or ;");
    }
}
s ~ /\<throw\>/ {
    if (!matchFile) {}
    else if (isCpp) {
        # cannot yet handle C++ cases like 'void foo() throw(int)'
    } else if (s !~ /^(    )+(throw)/) {
        error(fname, FNR, "throw must be correctly indented");
    } else if (s !~ /\<throw / && s !~ /\<throw$/) {
        error(fname, FNR, "throw must be followed by space");
    }
}
s ~ /\<else\>/ {
    if (!matchFile) {}
    else if (isCpp && s ~ /^# *else$/) {} # ignore "#else"
    else if (s !~ /^(    )+} else (if |{$|{ *\/\/|{ *\/\*)/) {
        error(fname, FNR, "else must be preceded by } and followed by { or if and correctly indented");
    }
}
s ~ /\<do\>/ {
    if (!matchFile) {}
    else if (s !~ /^(    )*do {/) {
        error(fname, FNR, "do must be followed by space {, and correctly indented");
    }
}
s ~ /\<try\>/ {
    if (!matchFile) {}
    else if (s !~ /^(    )+try {/) {
        error(fname, FNR, "try must be followed by space {, and correctly indented");
    }
}
s ~ /\<catch\>/ {
    if (!matchFile) {}
    else if (s !~ /^(    )+} catch /) {
        error(fname, FNR, "catch must be preceded by }, followed by space, and correctly indented");
    }
}
s ~ /\<finally\>/ {
    if (!matchFile) {}
    else if (s !~ /^(    )+} finally {/) {
        error(fname, FNR, "finally must be preceded by }, followed by space {, and correctly indented");
    }
}
s ~ /\($/ {
    nextIndent = countLeadingSpaces(s) + 4;
    if (s ~ / (if|while) .*\(.*\(/) {
        nextIndent += 4;
    }
}
match(s, /([]A-Za-z0-9()])(+|-|\*|\^|\/|%|=|==|+=|-=|\*=|\/=|>=|<=|!=|&|&&|\||\|\||^|\?|:) *[A-Za-z0-9(]/, a) {
    # < and > are not handled here - they have special treatment below
    if (!matchFile) {}
#    else if (s ~ /<.*>/) {} # ignore templates
    else if (a[2] == "-" && s ~ /\(-/) {} # ignore case "foo(-1)"
    else if (a[2] == "-" && s ~ /[eE][+-][0-9]/) {} # ignore e.g. 1e-5
    else if (a[2] == "+" && s ~ /[eE][+-][0-9]/) {} # ignore e.g. 1e+5
    else if (a[2] == ":" && s ~ /(case.*|default):$/) {} # ignore e.g. "case 5:"
    else if (isCpp && s ~ /[^ ][*&]/) {} # ignore e.g. "Foo* p;" in c++ - debatable
    else if (isCpp && s ~ /\<operator.*\(/) {} # ignore e.g. "operator++()" in c++
    else if (isCpp && a[2] == "/" && s ~ /#include/) {} # ignore e.g. "#include <x/y.hpp>" in c++
    else {
        error(fname, FNR, "operator '" a[2] "' must be preceded by space");
    }
}
match(s, /([]A-Za-z0-9() ] *)(+|-|\*|\^|\/|%|=|==|+=|-=|\*=|\/=|>=|<=|!=|&|&&|\||\|\||^|\?|:|,)[A-Za-z0-9(]/, a) {
    if (!matchFile) {}
#    else if (s ~ /<.*>/) {} # ignore templates
    else if (a[2] == "-" && s ~ /(\(|return |case |= )-/) {} # ignore prefix -
    else if (a[2] == ":" && s ~ /(case.*|default):$/) {} # ignore e.g. "case 5:"
    else if (s ~ /, *-/) {} # ignore case "foo(x, -1)"
    else if (s ~ /-[^ ]/ && s ~ /[^A-Za-z0-9] -/) {} # ignore case "x + -1" but not "x -1" or "3 -1"
    else if (a[2] == "-" && s ~ /[eE][+-][0-9]/) {} # ignore e.g. 1e-5
    else if (a[2] == "+" && s ~ /[eE][+-][0-9]/) {} # ignore e.g. 1e+5
    else if (a[2] == "*" && isCpp && s ~ /\*[^ ]/) {} # ignore e.g. "Foo *p;" in c++
    else if (a[2] == "&" && isCpp && s ~ /&[^ ]/) {} # ignore case "foo(&x)" in c++
    else if (isCpp && s ~ /\<operator[^ ]+\(/) {} # ignore e.g. "operator++()" in c++
    else if (isCpp && a[2] == "/" && s ~ /#include/) {} # ignore e.g. "#include <x/y.hpp>" in c++
    else if (lenient && fname ~ /(fennel)/ && a[1] = ",") {} # not enabled yet
    else {
        error(fname, FNR, "operator '" a[2] "' must be followed by space");
    }
}
match(s, /( )(,)/, a) {
    # (, < and > are not handled here - they have special treatment below
    if (!matchFile) {}
    else {
        error(fname, FNR, "operator '" a[2] "' must not be preceded by space");
    }
}
match(s, / (+|-|\*|\/|==|>=|<=|!=|<<|<<<|>>|&|&&|\|\||\?|:)$/, a) || \
match(s, /(\.|->)$/, a) {
    if (lenient && fname ~ /(aspen)/ && a[1] != ":") {} # not enabled yet
    else if (lenient && fname ~ /(fennel|farrago|aspen)/ && a[1] = "+") {} # not enabled yet
    else if (a[1] == ":" && s ~ /(case.*|default):$/) {
        # ignore e.g. "case 5:"
    } else if ((a[1] == "*" || a[1] == "&") && isCpp && s ~ /^[[:alnum:]:_ ]* [*&]$/) {
        # ignore e.g. "const int *\nClass::Subclass2::method(int x)"
    } else {
        error(fname, FNR, "operator '" a[1] "' must not be at end of line");
    }
}
match(s, /^ *(=) /, a) {
    error(fname, FNR, "operator '" a[1] "' must not be at start of line");
}
match(s, /([[:alnum:]~]+)( )([(])/, a) {
    # (, < and > are not handled here - they have special treatment below
    if (!matchFile) {}
    else if (isJava && a[1] ~ /\<(if|while|for|catch|switch|case|return|throw|synchronized|assert)\>/) {}
    else if (isCpp && a[1] ~ /\<(if|while|for|catch|switch|case|return|throw|operator|void|PBuffer)\>/) {}
    else if (isCpp && s ~ /^#define /) {}
    else {
        error(fname, FNR, "there must be no space before '" a[3] "' in fun call or fun decl");
    }
}
s ~ /\<[[:digit:][:lower:]][[:alnum:]_]*</ {
    # E.g. "p<" but not "Map<"
    if (!matchFile) {}
    else if (isCpp) {} # in C++ 'xyz<5>' could be a template
    else {
        error(fname, FNR, "operator '<' must be preceded by space");
    }
}
s ~ /\<[[:digit:][:lower:]][[:alnum:]_]*>/ {
    # E.g. "g>" but not "String>" as in "List<String>"
    if (!matchFile) {}
    else if (isCpp) {} # in C++ 'xyz<int>' could be a template
    else {
        error(fname, FNR, "operator '>' must be preceded by space");
    }
}
match(s, /<([[:digit:][:lower:]][[:alnum:].]*)\>/, a) {
    if (!matchFile) {}
    else if (isCpp) {
        # in C++, template and include generate too many false positives
    } else if (isJava && a[1] ~ /(int|char|long|boolean|byte|double|float)/) {
        # Allow e.g. 'List<int[]>'
    } else if (isJava && a[1] ~ /^[[:lower:]]+\./) {
        # Allow e.g. 'List<java.lang.String>'
    } else {
        error(fname, FNR, "operator '<' must be followed by space");
    }
}
match(s, /^(.*[^-])>([[:digit:][:lower:]][[:alnum:]]*)\>/, a) {
    if (!matchFile) {}
    else if (isJava && a[1] ~ /.*\.<.*/) {
        # Ignore 'Collections.<Type>member'
    } else {
        error(fname, FNR, "operator '>' must be followed by space");
    }
}
s ~ /[[(] / {
    if (!matchFile) {}
    else if (s ~ /[[(] +\\$/) {} # ignore '#define foo(   \'
    else {
        error(fname, FNR, "( or [ must not be followed by space");
    }
}
s ~ / [])]/ {
    if (!matchFile) {}
    else if (s ~ /^ *\)/ && previousLineEndedInCloseBrace) {} # ignore "bar(new Foo() { } );"
    else {
        error(fname, FNR, ") or ] must not be followed by space");
    }
}
s ~ /}/ {
    if (!matchFile) {}
    else if (s !~ /}( |;|,|$|\))/) {
        error(fname, FNR, "} must be followed by space");
    } else if (s !~ /(    )*}/) {
        error(fname, FNR, "} must be at start of line and correctly indented");
    }
}
s ~ /{/ {
    if (!matchFile) {}
    else if (s ~ /(\]\)?|=) *{/) {} # ignore e.g. "(int[]) {1, 2}" or "int[] x = {1, 2}"
    else if (s ~ /\({/) {} # ignore e.g. @SuppressWarnings({"unchecked"})
    else if (s ~ /{ *(\/\/|\/\*)/) {} # ignore e.g. "do { // a comment"
    else if (s ~ / {}$/) {} # ignore e.g. "Constructor() {}"
    else if (s ~ / },$/) {} # ignore e.g. "{ yada },"
    else if (s ~ / };$/) {} # ignore e.g. "{ yada };"
    else if (s ~ / {};$/) {} # ignore e.g. "template <> class Foo<int> {};"
    else if (s ~ / },? *\/\/.*$/) {} # ignore e.g. "{ yada }, // comment"
    else if (s ~ /\\$/) {} # ignore multiline macros
    else if (s ~ /{}/) { # e.g. "Constructor(){}"
        error(fname, FNR, "{} must be preceded by space and at end of line");
    } else if (isCpp && s ~ /{ *\\$/) {
        # ignore - "{" can be followed by "\" in c macro
    } else if (s !~ /{$/) {
        error(fname, FNR, "{ must be at end of line");
    } else if (s !~ /(^| ){/) {
        error(fname, FNR, "{ must be preceded by space or at start of line");
    } else {
        opens = s;
        gsub(/[^(]/, "", opens);
        closes = s;
        gsub(/[^)]/, "", closes);
        if (0 && lenient && fname ~ /aspen/) {} # not enabled
        else if (length(closes) > length(opens)) {
            error(fname, FNR, "Open brace should be on new line (function call/decl spans several lines)");
        }
    }
}
s ~ /(^| )(class|interface|enum) / ||
s ~ /(^| )namespace / && isCpp {
    if (isCpp && s ~ /;$/) {} # ignore type declaration
    else {
        classDeclStartLine = FNR;
        t = s;
        gsub(/.*(class|interface|enum|namespace) /, "", t);
        gsub(/ .*$/, "", t);
        if (s ~ /template/) {
            # ignore case "template <class INSTCLASS> static void foo()"
            classDeclStartLine = 0;
        } else if (t ~ /[[:upper:]][[:upper:]][[:upper:]][[:upper:]]/     \
            && t !~ /LRU/ \
            && t !~ /WAL/ \
            && t !~ /classUUID/ \
            && t !~ /classSQLException/ \
            && t !~ /BBRC/ \
            && t !~ /_/ \
            && t !~ /EncodedSqlInterval/)
        {
            error(fname, FNR, "Class name " t " has consecutive uppercase letters");
        }
    }
}
s ~ / throws\>/ {
    if (s ~ /\(/) {
        funDeclStartLine = FNR;
    } else {
        funDeclStartLine = FNR - 1;
    }
}
length($0) > maxLineLength                      \
&& $0 !~ /@(throws|see|link)/                   \
&& $0 !~ /\$Id: /                               \
&& $0 !~ /^import /                             \
&& $0 !~ /http:/                                \
&& $0 !~ /\/\/ Expect "/                        \
&& s !~ /^ *(\+ |<< )?string\)?[;,]?$/ {
    error( \
        fname, \
        FNR, \
        "Line length (" length($0) ") exceeds " maxLineLength " chars");
}
/}$/ {
    previousLineEndedInCloseBrace = 2;
}
/;$/ {
    funDeclStartLine = 0;
}
/{$/ {
    # Ignore open brace if it is part of class or interface declaration.
    if (classDeclStartLine) {
        if (classDeclStartLine < FNR \
            && $0 !~ /^ *{$/)
        {
            error(fname, FNR, "Open brace should be on new line (class decl spans several lines)");
        }
        classDeclStartLine = 0;
    } else {
        previousLineEndedInOpenBrace = 2;
    }
    if (funDeclStartLine) {
        if (funDeclStartLine < FNR \
            && $0 !~ /^ *{$/)
        {
            if (lenient && fname ~ /aspen/) {} # not enabled
            else error(fname, FNR, "Open brace should be on new line (function decl spans several lines)");
        }
        funDeclStartLine = 0;
    }
}
/^$/ {
    previousLineWasEmpty = 2;
}
{
    next;
}
END {
    afterFile();
}

# End checkFile.awk
