package javax. olap. query. enumerations;
import java. util.*;
public class EdgeFilterTypeEnum implements EdgeFilterType {
public static final EdgeFilterTypeEnum TUPLEFILTER = new EdgeFilterTypeEnum(" TUPLEFILTER");
public static final EdgeFilterTypeEnum EDGEDRILLFILTER = new EdgeFilterTypeEnum(" EDGEDRILLFILTER");
public static final EdgeFilterTypeEnum SUPPRESSORDINATEMEMBERFILTER = new EdgeFilterTypeEnum(" SUPPRESSORDINATEMEMBERFILTER");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" TUPLEFILTER");
temp. add(" EDGEDRILLFILTER"); temp. add(" SUPPRESSORDINATEMEMBERFILTER");
typeName = java. util. Collections. unmodifiableList( temp); }


private EdgeFilterTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }
public boolean equals( Object o) {
if( o instanceof EdgeFilterTypeEnum) return (o == this); else if( o instanceof EdgeFilterType) return
(o. toString(). equals( literalName)); else return( false);
}
}

