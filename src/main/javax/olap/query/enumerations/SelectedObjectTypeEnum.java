package javax. olap. query. enumerations;
import java. util.*;
public class SelectedObjectTypeEnum implements SelectedObjectType {
public static final SelectedObjectTypeEnum ATTRIBUTEREFERENCE = new SelectedObjectTypeEnum(" ATTRIBUTEREFERENCE");
public static final SelectedObjectTypeEnum DERIVEDATTRIBUTE = new SelectedObjectTypeEnum(" DERIVEDATTRIBUTE");
public static final SelectedObjectTypeEnum LITERALREFERENCE = new SelectedObjectTypeEnum(" LITERALREFERENCE");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" ATTRIBUTEREFERENCE");
temp. add(" DERIVEDATTRIBUTE"); temp. add(" LITERALREFERENCE");
typeName = java. util. Collections. unmodifiableList( temp); }


private SelectedObjectTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o)
{ if( o instanceof SelectedObjectTypeEnum) return (o == this);
else if( o instanceof SelectedObjectType) return (o. toString(). equals( literalName));
else return( false); }


}

