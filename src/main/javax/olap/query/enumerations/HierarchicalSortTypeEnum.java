package javax. olap. query. enumerations;
import java. util.*;
public class HierarchicalSortTypeEnum implements HierarchicalSortType {
public static final HierarchicalSortTypeEnum PARENTSFIRST = new HierarchicalSortTypeEnum(" PARENTSFIRST");
public static final HierarchicalSortTypeEnum CHILDRENFIRST = new HierarchicalSortTypeEnum(" CHILDRENFIRST");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" PARENTSFIRST");
temp. add(" CHILDRENFIRST"); typeName = java. util. Collections. unmodifiableList( temp);
}
private HierarchicalSortTypeEnum( String literalName) {
this. literalName = literalName; }
public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof HierarchicalSortTypeEnum) return (o == this); else if( o instanceof HierarchicalSortType) return
(o. toString(). equals( literalName)); else return( false);
}
}


