package javax. olap. query. enumerations;
import java. util.*;
public class DataBasedMemberFilterInputTypeEnum implements DataBasedMemberFilterInputType
{ public static final DataBasedMemberFilterInputTypeEnum
ATTRIBUTEREFERENCE = new DataBasedMemberFilterInputTypeEnum(" ATTRIBUTEREFERENCE");
public static final DataBasedMemberFilterInputTypeEnum QUALIFIEDMEMBERREFERENCE = new
DataBasedMemberFilterInputTypeEnum(" QUALIFIEDMEMBERREFERENCE"); public static final DataBasedMemberFilterInputTypeEnum
DERIVEDATTRIBUTE = new DataBasedMemberFilterInputTypeEnum(" DERIVEDATTRIBUTE");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" ATTRIBUTEREFERENCE");
temp. add(" QUALIFIEDMEMBERREFERENCE"); temp. add(" DERIVEDATTRIBUTE");
typeName = java. util. Collections. unmodifiableList( temp); }


private DataBasedMemberFilterInputTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof DataBasedMemberFilterInputTypeEnum) return (o == this);
else if( o instanceof DataBasedMemberFilterInputType) return (o. toString(). equals( literalName));
else return( false); }


}

