package javax. olap. query. enumerations;
import java. util.*;
public class EdgeInsertOffsetTypeEnum implements EdgeInsertOffsetType {
public static final EdgeInsertOffsetTypeEnum INTEGERINSERTOFFSET = new EdgeInsertOffsetTypeEnum(" INTEGERINSERTOFFSET");
public static final EdgeInsertOffsetTypeEnum TUPLEINSERTOFFSET = new EdgeInsertOffsetTypeEnum(" TUPLEINSERTOFFSET");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" INTEGERINSERTOFFSET");
temp. add(" TUPLEINSERTOFFSET"); typeName = java. util. Collections. unmodifiableList( temp);
}
private EdgeInsertOffsetTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }
public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof EdgeInsertOffsetTypeEnum) return (o == this); else if( o instanceof EdgeInsertOffsetType) return
(o. toString(). equals( literalName)); else return( false);
}
}


