package javax. olap. query. enumerations;
import java. util.*;
public class DrillTypeEnum implements DrillType {
public static final DrillTypeEnum PARENT = new DrillTypeEnum(" PARENT");
public static final DrillTypeEnum ANCESTORS = new DrillTypeEnum(" ANCESTORS");
public static final DrillTypeEnum DESCENDANTS = new DrillTypeEnum(" DESCENDANTS");
public static final DrillTypeEnum SIBLINGS = new DrillTypeEnum(" SIBLINGS");
public static final DrillTypeEnum CHILDREN = new DrillTypeEnum(" CHILDREN");
public static final DrillTypeEnum ROOTS = new DrillTypeEnum(" ROOTS"); public static final DrillTypeEnum LEAVES = new
DrillTypeEnum(" LEAVES"); public static final DrillTypeEnum TOLEVEL = new
DrillTypeEnum(" TOLEVEL"); private static final List typeName;
private final String literalName; static
{ java. util. ArrayList temp = new java. util. ArrayList();
temp. add(" PARENT"); temp. add(" ANCESTORS");
temp. add(" DESCENDANTS"); temp. add(" SIBLINGS");
temp. add(" CHILDREN"); temp. add(" ROOTS");
temp. add(" LEAVES"); temp. add(" TOLEVEL");
typeName = java. util. Collections. unmodifiableList( temp); }


private DrillTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof DrillTypeEnum) return (o == this); else if( o instanceof DrillType) return
(o. toString(). equals( literalName)); else return( false);
}
}

