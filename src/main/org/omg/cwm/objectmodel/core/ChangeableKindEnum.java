package org. omg. cwm. objectmodel. core;
import java. util.*;
public class ChangeableKindEnum implements ChangeableKind {
public static final ChangeableKindEnum CK_CHANGEABLE = new ChangeableKindEnum(" CK_CHANGEABLE");
public static final ChangeableKindEnum CK_FROZEN = new ChangeableKindEnum(" CK_FROZEN");
public static final ChangeableKindEnum CK_ADDONLY = new ChangeableKindEnum(" CK_ADDONLY");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" CK_CHANGEABLE");
temp. add(" CK_FROZEN"); temp. add(" CK_ADDONLY");
typeName = java. util. Collections. unmodifiableList( temp); }


private ChangeableKindEnum( String literalName) {
this. literalName = literalName; }
public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof ChangeableKindEnum) return (o == this); else if( o instanceof ChangeableKind) return
(o. toString(). equals( literalName)); else return( false);
}
}


