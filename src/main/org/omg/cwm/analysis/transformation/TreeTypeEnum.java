package org. omg. cwm. analysis. transformation;
import java. util.*;
public class TreeTypeEnum implements TreeType {
public static final TreeTypeEnum TFM_UNARY = new TreeTypeEnum(" TFM_UNARY");
public static final TreeTypeEnum TFM_BINARY = new TreeTypeEnum(" TFM_BINARY");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" TFM_UNARY");
temp. add(" TFM_BINARY"); typeName = java. util. Collections. unmodifiableList( temp);
}
private TreeTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof TreeTypeEnum) return (o == this); else if( o instanceof TreeType) return
(o. toString(). equals( literalName)); else return( false);
}
}