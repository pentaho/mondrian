
package javax. olap. query. enumerations;
import java. util.*;
public class DimensionInsertOffsetTypeEnum implements DimensionInsertOffsetType
{ public static final DimensionInsertOffsetTypeEnum INTEGERINSERTOFFSET
= new DimensionInsertOffsetTypeEnum(" INTEGERINSERTOFFSET"); public static final DimensionInsertOffsetTypeEnum MEMBERINSERTOFFSET
= new DimensionInsertOffsetTypeEnum(" MEMBERINSERTOFFSET"); private static final List typeName;
private final String literalName; static
{ java. util. ArrayList temp = new java. util. ArrayList();
temp. add(" INTEGERINSERTOFFSET"); temp. add(" MEMBERINSERTOFFSET");
typeName = java. util. Collections. unmodifiableList( temp); }


private DimensionInsertOffsetTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof DimensionInsertOffsetTypeEnum) return (o == this);
else if( o instanceof DimensionInsertOffsetType) return (o. toString(). equals( literalName));
else return( false); }


}

