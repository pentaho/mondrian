package javax. olap. query. enumerations;
import java. util.*;
public class DerivedAttributeComponentTypeEnum implements DerivedAttributeComponentType
{ public static final DerivedAttributeComponentTypeEnum
ATTRIBUTEREFERENCE = new DerivedAttributeComponentTypeEnum(" ATTRIBUTEREFERENCE");
public static final DerivedAttributeComponentTypeEnum LITERALREFERENCE = new
DerivedAttributeComponentTypeEnum(" LITERALREFERENCE"); public static final DerivedAttributeComponentTypeEnum
DERIVEDATTRIBUTE = new DerivedAttributeComponentTypeEnum(" DERIVEDATTRIBUTE");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" ATTRIBUTEREFERENCE");
temp. add(" LITERALREFERENCE"); temp. add(" DERIVEDATTRIBUTE");
typeName = java. util. Collections. unmodifiableList( temp); }


private DerivedAttributeComponentTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof DerivedAttributeComponentTypeEnum) return (o == this);
else if( o instanceof DerivedAttributeComponentType) return (o. toString(). equals( literalName));
else return( false); }


}
