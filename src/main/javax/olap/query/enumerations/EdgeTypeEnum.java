package javax. olap. query. enumerations;
import java. util.*;
public class EdgeTypeEnum implements EdgeType {
public static final EdgeTypeEnum ROW = new EdgeTypeEnum(" ROW"); public static final EdgeTypeEnum COLUMN = new EdgeTypeEnum(" COLUMN");
public static final EdgeTypeEnum PAGE = new EdgeTypeEnum(" PAGE"); public static final EdgeTypeEnum SLICER = new EdgeTypeEnum(" SLICER");
public static final EdgeTypeEnum OTHER = new EdgeTypeEnum(" OTHER"); private static final List typeName;
private final String literalName; static
{ java. util. ArrayList temp = new java. util. ArrayList();
temp. add(" ROW"); temp. add(" COLUMN");
temp. add(" PAGE"); temp. add(" SLICER");
temp. add(" OTHER"); typeName = java. util. Collections. unmodifiableList( temp);
}
private EdgeTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }
public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof EdgeTypeEnum) return (o == this); else if( o instanceof EdgeType) return
(o. toString(). equals( literalName)); else return( false);
}
}


