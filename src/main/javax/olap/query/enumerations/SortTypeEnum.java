package javax. olap. query. enumerations;
import java. util.*;
public class SortTypeEnum implements SortType {
public static final SortTypeEnum ASCENDING = new SortTypeEnum(" ASCENDING");
public static final SortTypeEnum DESCENDING = new SortTypeEnum(" DESCENDING");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" ASCENDING");
temp. add(" DESCENDING"); typeName = java. util. Collections. unmodifiableList( temp);
}
private SortTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }
public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof SortTypeEnum) return (o == this); else if( o instanceof SortType) return
(o. toString(). equals( literalName)); else return( false);
}
}


