package javax. olap. query. enumerations;
import java. util.*;
public class HierarchyFilterTypeEnum implements HierarchyFilterType {
public static final HierarchyFilterTypeEnum ALLMEMBERS = new HierarchyFilterTypeEnum(" ALLMEMBERS");
public static final HierarchyFilterTypeEnum BOTTOMLEVEL = new HierarchyFilterTypeEnum(" BOTTOMLEVEL");
public static final HierarchyFilterTypeEnum TOPLEVEL = new HierarchyFilterTypeEnum(" TOPLEVEL");
public static final HierarchyFilterTypeEnum LEAFMEMBERS = new HierarchyFilterTypeEnum(" LEAFMEMBERS");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" ALLMEMBERS");
temp. add(" BOTTOMLEVEL"); temp. add(" TOPLEVEL");
temp. add(" LEAFMEMBERS"); typeName = java. util. Collections. unmodifiableList( temp);
}
private HierarchyFilterTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof HierarchyFilterTypeEnum) return (o == this); else if( o instanceof HierarchyFilterType) return
(o. toString(). equals( literalName)); else return( false);
}
}


