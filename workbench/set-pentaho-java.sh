#!/bin/sh
# -----------------------------------------------------------------------------
# Finds a suitable Java
#
# Looks in well-known locations to find a suitable Java then sets two 
# environment variables for use in other script files. The two environment
# variables are:
# 
# * _PENTAHO_JAVA_HOME - absolute path to Java home
# * _PENTAHO_JAVA - absolute path to Java launcher (e.g. java)
# 
# The order of the search is as follows:
# 
# 1. environment variable PENTAHO_JAVA_HOME - path to Java home
# 2. environment variable JAVA_HOME - path to Java home
# 3. environment variable JRE_HOME - path to Java home
# 4. argument #1 - path to Java home
# 
# If a suitable Java is found at one of these locations, then 
# _PENTAHO_JAVA_HOME is set to that location and _PENTAHO_JAVA is set to the 
# absolute path of the Java launcher at that location. If none of these 
# locations are suitable, then _PENTAHO_JAVA_HOME is set to empty string and 
# _PENTAHO_JAVA is set to java.
# 
# Finally, there is one final optional environment variable: PENTAHO_JAVA.
# If set, this value is used in the construction of _PENTAHO_JAVA. If not 
# set, then the value java is used. 
# -----------------------------------------------------------------------------

setPentahoJava() {
  if [ -n "$PENTAHO_JAVA" ]; then
    __LAUNCHER="$PENTAHO_JAVA"
  else
    __LAUNCHER="java"
  fi
  if [ -n "$PENTAHO_JAVA_HOME" ]; then
    echo "DEBUG: Using PENTAHO_JAVA_HOME"
    _PENTAHO_JAVA_HOME="$PENTAHO_JAVA_HOME"
    _PENTAHO_JAVA="$_PENTAHO_JAVA_HOME"/bin/$__LAUNCHER
  elif [ -n "$JAVA_HOME" ]; then
    echo "DEBUG: Using JAVA_HOME"
    _PENTAHO_JAVA_HOME="$JAVA_HOME"
    _PENTAHO_JAVA="$_PENTAHO_JAVA_HOME"/bin/$__LAUNCHER
  elif [ -n "$JRE_HOME" ]; then
    echo "DEBUG: Using JRE_HOME"
    _PENTAHO_JAVA_HOME="$JRE_HOME"
    _PENTAHO_JAVA="$_PENTAHO_JAVA_HOME"/bin/$__LAUNCHER
  elif [ -n "$1" ] && [ -x "$1"/bin/$__LAUNCHER ]; then
    echo "DEBUG: Using value ($1) from calling script"
    _PENTAHO_JAVA_HOME="$1"
    _PENTAHO_JAVA="$_PENTAHO_JAVA_HOME"/bin/$__LAUNCHER
  else
    echo "WARNING: Using java from path"
    _PENTAHO_JAVA_HOME=
    _PENTAHO_JAVA=$__LAUNCHER
  fi
  echo "DEBUG: _PENTAHO_JAVA_HOME=$_PENTAHO_JAVA_HOME"
  echo "DEBUG: _PENTAHO_JAVA=$_PENTAHO_JAVA"
}