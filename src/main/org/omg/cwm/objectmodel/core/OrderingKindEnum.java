package org. omg. cwm. objectmodel. core;
import java. util.*;
public class OrderingKindEnum implements OrderingKind {
public static final OrderingKindEnum OK_UNORDERED = new OrderingKindEnum(" OK_UNORDERED");
public static final OrderingKindEnum OK_ORDERED = new OrderingKindEnum(" OK_ORDERED");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" OK_UNORDERED");
temp. add(" OK_ORDERED"); typeName = java. util. Collections. unmodifiableList( temp);
}
private OrderingKindEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof OrderingKindEnum) return (o == this); else if( o instanceof OrderingKind) return
(o. toString(). equals( literalName)); else return( false);
}
}


