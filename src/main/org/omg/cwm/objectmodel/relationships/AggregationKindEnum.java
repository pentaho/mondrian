package org. omg. cwm. objectmodel. relationships;
import java. util.*;
public class AggregationKindEnum implements AggregationKind {
public static final AggregationKindEnum AK_NONE = new AggregationKindEnum(" AK_NONE");
public static final AggregationKindEnum AK_AGGREGATE = new AggregationKindEnum(" AK_AGGREGATE");
public static final AggregationKindEnum AK_COMPOSITE = new AggregationKindEnum(" AK_COMPOSITE");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" AK_NONE");
temp. add(" AK_AGGREGATE"); temp. add(" AK_COMPOSITE");
typeName = java. util. Collections. unmodifiableList( temp); }


private AggregationKindEnum( String literalName) {
this. literalName = literalName; }
public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof AggregationKindEnum) return (o == this); else if( o instanceof AggregationKind) return
(o. toString(). equals( literalName)); else return( false);
}
}


