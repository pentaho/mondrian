package org. omg. cwm. objectmodel. core;
import java. util.*;
public class VisibilityKindEnum implements VisibilityKind {
public static final VisibilityKindEnum VK_PUBLIC = new VisibilityKindEnum(" VK_PUBLIC");
public static final VisibilityKindEnum VK_PROTECTED = new VisibilityKindEnum(" VK_PROTECTED");
public static final VisibilityKindEnum VK_PRIVATE = new VisibilityKindEnum(" VK_PRIVATE");
public static final VisibilityKindEnum VK_PACKAGE = new VisibilityKindEnum(" VK_PACKAGE");
public static final VisibilityKindEnum VK_NOTAPPLICABLE = new VisibilityKindEnum(" VK_NOTAPPLICABLE");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" VK_PUBLIC");
temp. add(" VK_PROTECTED"); temp. add(" VK_PRIVATE");
temp. add(" VK_PACKAGE"); temp. add(" VK_NOTAPPLICABLE");
typeName = java. util. Collections. unmodifiableList( temp); }


private VisibilityKindEnum( String literalName) {
this. literalName = literalName; }
public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof VisibilityKindEnum) return (o == this); else if( o instanceof VisibilityKind) return
(o. toString(). equals( literalName)); else return( false);
}
}


