#!/bin/bash
#
# Checks that a file is valid.
# Used by perforce submit trigger, via runTrigger.
# The file is deemed to be valid if this command produces no output.
#
# Usage:
#   checkFile [ --depotPath <depotPath> ] <file> 
#
# runTrigger uses first form, with a temporary file, e.g.
#   checkFile --depotPath //depot/src/foo/Bar.java /tmp/foo.txt
#
# The second form is useful for checking files in the client before you
# try to submit them:
#   checkFile src/foo/Bar.java
#

usage() {
    echo "checkFile  [ <options> ] --depotPath <depotPath> <file>"
    echo "    Checks a temporary file. depotPath is the full path of"
    echo "    the file stored in perforce, for error reporting; file"
    echo "    holds the actual file contents."
    echo "checkFile  [ <options> ] <file>..."
    echo "    Checks a list of files."
    echo "checkFile  [ <options> ] --opened"
    echo "    Checks all files that are opened in git. Implies --strict."
    echo "checkFile  [ <options> ] --under <dir>"
    echo "    Recursively checks all files under a given directory."
    echo "checkFile --help"
    echo "    Prints this help."
    echo
    echo "Options:"
    echo "--lenient"
    echo "    Does not apply rules to components which are not known to"
    echo "    be in compliance. The perforce trigger uses this option."
    echo "--strict"
    echo "    Stricter than usual; the opposite of lenient."
}

doCheck() {
    filePath="$1"
    file="$2"
    maxLineLength="$3"

    # CHECKFILE_IGNORE is an environment variable that contains a callback
    # to decide whether to check this file. The command or function should
    # succeed (that is, return 0) if checkFile is to ignore the file, fail
    # (that is, return 1 or other non-zero value) otherwise.
    if [ "$CHECKFILE_IGNORE" ]; then
        if eval $CHECKFILE_IGNORE "$filePath"; then
            return
        fi
    fi

    # Exceptions for mondrian
    case "$filePath" in
    */mondrian/util/Base64.java| \
    */mondrian/olap/MondrianDef.java| \
    */mondrian/olap/Mondrian3Def.java| \
    */mondrian/xmla/DataSourcesConfig.java| \
    */mondrian/rolap/aggmatcher/DefaultDef.java| \
    */mondrian/resource/MondrianResource*.java| \
    */mondrian/olap/MondrianProperties.java| \
    */mondrian/olap/Parser.java| \
    */mondrian/olap/ParserSym.java| \
    */mondrian/server/MondrianServerVersion.java| \
    */mondrian/parser/ParseException.java| \
    */mondrian/parser/Token.java| \
    */mondrian/parser/TokenMgrError.java| \
    */mondrian/parser/SimpleCharStream.java| \
    */mondrian/parser/MdxParserImplConstants.java| \
    */mondrian/parser/MdxParserImplTokenManager.java| \
    */mondrian/parser/MdxParserImpl.java)
        # mondrian.util.Base64 is checked in as is, so don't check it
        # Other files above are generated.
        return
        ;;

    # Exceptions for olap4j
    */org/olap4j/mdx/parser/impl/*.java| \
    */org/olap4j/impl/Base64.java)
        return
        ;;

    # Exceptions for farrago
    */farrago/classes/* | \
    */farrago/catalog/* | \
    */farrago/examples/rng/src/net/sf/farrago/rng/parserimpl/*.java | \
    */farrago/examples/rng/src/net/sf/farrago/rng/resource/FarragoRngResource*.java | \
    */farrago/examples/rng/catalog/net/sf/farrago/rng/rngmodel/* | \
    */farrago/examples/rng/catalog/java/* | \
    */farrago/jdbc4/*.java | \
    */farrago/src/net/sf/farrago/FarragoMetadataFactoryImpl.java | \
    */farrago/src/net/sf/farrago/FarragoMetadataFactory.java | \
    */farrago/src/net/sf/farrago/parser/impl/*.java | \
    */farrago/src/net/sf/farrago/resource/FarragoResource*.java | \
    */farrago/src/net/sf/farrago/resource/FarragoInternalQuery*.java | \
    */farrago/src/net/sf/farrago/test/FarragoSqlTestWrapper.java | \
    */farrago/src/org/eigenbase/jdbc4/*.java | \
    */farrago/src/org/eigenbase/lurql/parser/*.java | \
    */farrago/src/com/lucidera/lurql/parser/*.java | \
    */farrago/src/org/eigenbase/resource/EigenbaseResource*.java | \
    */farrago/src/org/eigenbase/sql/parser/impl/*.java)
        return
        ;;

    # Exceptions for fennel
    */fennel/CMakeFiles/CompilerIdCXX/CMakeCXXCompilerId.cpp | \
    */fennel/disruptivetech/calc/CalcGrammar.tab.cpp | \
    */fennel/disruptivetech/calc/CalcGrammar.cpp | \
    */fennel/disruptivetech/calc/CalcGrammar.h | \
    */fennel/disruptivetech/calc/CalcLexer.cpp | \
    */fennel/disruptivetech/calc/CalcLexer.h | \
    */fennel/calculator/CalcGrammar.tab.cpp | \
    */fennel/calculator/CalcGrammar.cpp | \
    */fennel/calculator/CalcGrammar.h | \
    */fennel/calculator/CalcLexer.cpp | \
    */fennel/calculator/CalcLexer.h | \
    */fennel/common/FemGeneratedEnums.h | \
    */fennel/common/FennelResource.cpp | \
    */fennel/common/FennelResource.h | \
    */fennel/config.h | \
    */fennel/farrago/FemGeneratedClasses.h | \
    */fennel/farrago/FemGeneratedMethods.h | \
    */fennel/farrago/NativeMethods.h)
        return
        ;;

    # Skip our own test files, unless we are testing.
    */util/test/CheckFile1.*)
        if [ ! "$test" ]; then
            return
        fi
        ;;

    # Only validate .java and parser files at present.
    *.java|*.cup|*.h|*.cpp)
        ;;

    *)
        return
        ;;
    esac

    # Set maxLineLength if it is not already set. ('checkFile --opened'
    # sets it to the strictest value, 80).
    if [ ! "$maxLineLength" ]; then
        maxLineLength=80
    fi

    # Check whether there are tabs, or lines end with spaces
    # todo: check for ' ;'
    # todo: check that every file has copyright/license header
    # todo: check that every class has javadoc
    # todo: check that every top-level class has @author and @version
    # todo: check c++ files
    if test "$deferred" ; then
        echo "$file" >> "${deferred_file}"
    else
        gawk -f "$CHECKFILE_AWK" \
            -v fname="$filePath" \
            -v strict="$strict" \
            -v maxLineLength="$maxLineLength" \
            "$file"
    fi
}

doCheckDeferred() {
    if [ -s "${deferred_file}" ]; then
        maxLineLength=80
        cat "${deferred_file}" |
        xargs -P $(expr ${CORE_COUNT} \* 2) -n 100 gawk -f "$CHECKFILE_AWK" \
            -v strict="$strict" \
            -v maxLineLength="$maxLineLength"
   fi
   rm -f "${deferred_file}"
}

function guessCoreCount() {
    if [ -f /proc/cpuinfo ]; then
         cat /proc/cpuinfo | awk '$1 == "processor"' | wc -l
    else
         # File doe not exist on Darwin or cygwin
         echo 2
    fi
}

export CORE_COUNT=$(guessCoreCount)

export deferred=true

# 'test' is an undocumented flag, overriding the default behavior which is
# to ignore our own test files
test=
if [ "$1" == --test ]; then
    test=true
    shift
fi

strict=1
if [ "$1" == --lenient ]; then
    strict=0
    shift
fi

if [ "$1" == --help ]; then
    usage
    exit 0
fi

if [ "$1" == --strict ]; then
    strict=2
    shift
fi

depotPath=
if [ "$1" == --depotPath ]; then
    depotPath="$2"
    deferred=
    shift 2
fi

opened=
if [ "$1" == --opened ]; then
    opened=true
    deferred=
    strict=2
    shift
fi

under=
if [ "$1" == --under ]; then
    if [ "$opened" ]; then
        echo "Cannot specify both --under and --opened"
        exit 1
    fi
    if [ ! -d "$2" ]; then
        echo "--under requires a directory; '$2' not found"
        exit 1
    fi
    under="$2"
    shift 2
fi

if [ "$1" == --opened ]; then
    echo "Cannot specify both --under and --opened"
    exit 1
fi

if [ ! -f "$CHECKFILE_AWK" ]
then
    case $(uname) in
    (Darwin)
        export CHECKFILE_AWK="$(cd $(dirname $0); pwd -P)/checkFile.awk";;
    (*)
        export CHECKFILE_AWK="$(dirname $(readlink -f $0))/checkFile.awk";;
    esac
fi

export deferred_file=/tmp/checkFile_deferred_$$.txt
rm -f "${deferred_file}"

(
if [ "$under" ]; then
    find "$under" -type f |
    while read file; do
        filePath="$file"
        if [ "$depotPath" ]; then
            filePath="$depotPath"
        fi
        doCheck "$filePath" "$file" ""
    done
elif [ "$opened" ]; then
    git checkout |
    gawk '$1 != "D" {print $2}' |
    while read file; do
        doCheck "$file" "$file" "80"
    done
else
    for file in "$@"; do
        filePath="$file"
        if [ "$depotPath" ]; then
            filePath="$depotPath"
        fi
        doCheck "$filePath" "$file" ""
    done
fi

if test "$deferred"; then
    doCheckDeferred
fi
) | tee /tmp/checkFile_output_$$.txt

status=0
if [ -s /tmp/checkFile_output_$$.txt ]; then
    status=1
fi
rm -f /tmp/checkFile_output_$$.txt

exit $status

# End checkFile
