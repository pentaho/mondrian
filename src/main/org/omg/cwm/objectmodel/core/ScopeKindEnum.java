package org. omg. cwm. objectmodel. core;
import java. util.*;
public class ScopeKindEnum implements ScopeKind {
public static final ScopeKindEnum SK_INSTANCE = new ScopeKindEnum(" SK_INSTANCE");
public static final ScopeKindEnum SK_CLASSIFIER = new ScopeKindEnum(" SK_CLASSIFIER");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" SK_INSTANCE");
temp. add(" SK_CLASSIFIER"); typeName = java. util. Collections. unmodifiableList( temp);
}
private ScopeKindEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }
public boolean equals( Object o) {
if( o instanceof ScopeKindEnum) return (o == this); else if( o instanceof ScopeKind) return
(o. toString(). equals( literalName)); else return( false);
}
}

