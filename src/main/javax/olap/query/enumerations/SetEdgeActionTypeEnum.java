package javax. olap. query. enumerations;
import java. util.*;
public class SetEdgeActionTypeEnum implements SetEdgeActionType {
public static final SetEdgeActionTypeEnum INITIAL = new SetEdgeActionTypeEnum(" INITIAL");
public static final SetEdgeActionTypeEnum APPEND = new SetEdgeActionTypeEnum(" APPEND");
public static final SetEdgeActionTypeEnum PREPEND = new SetEdgeActionTypeEnum(" PREPEND");
public static final SetEdgeActionTypeEnum DIFFERENCE = new SetEdgeActionTypeEnum(" DIFFERENCE");
public static final SetEdgeActionTypeEnum INTERSECTION = new SetEdgeActionTypeEnum(" INTERSECTION");
private static final List typeName; private final String literalName;
static { java. util. ArrayList temp = new java. util. ArrayList();
temp. add(" INITIAL"); temp. add(" APPEND");
temp. add(" PREPEND"); temp. add(" DIFFERENCE");
temp. add(" INTERSECTION"); typeName = java. util. Collections. unmodifiableList( temp);
}
private SetEdgeActionTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof SetEdgeActionTypeEnum) return (o == this); else if( o instanceof SetEdgeActionType) return
(o. toString(). equals( literalName)); else return( false);
}
}


