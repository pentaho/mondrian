package javax. olap. query. enumerations;
import java. util.*;
public class SetActionTypeEnum implements SetActionType {
public static final SetActionTypeEnum INITIAL = new SetActionTypeEnum(" INITIAL");
public static final SetActionTypeEnum APPEND = new SetActionTypeEnum(" APPEND");
public static final SetActionTypeEnum PREPEND = new SetActionTypeEnum(" PREPEND");
public static final SetActionTypeEnum INSERT = new SetActionTypeEnum(" INSERT");
public static final SetActionTypeEnum DIFFERENCE = new SetActionTypeEnum(" DIFFERENCE");
public static final SetActionTypeEnum INTERSECTION = new SetActionTypeEnum(" INTERSECTION");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" INITIAL");
temp. add(" APPEND"); temp. add(" PREPEND");
temp. add(" INSERT"); temp. add(" DIFFERENCE");
temp. add(" INTERSECTION"); typeName = java. util. Collections. unmodifiableList( temp);
}
private SetActionTypeEnum( String literalName) {
this. literalName = literalName;
}
public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof SetActionTypeEnum) return (o == this); else if( o instanceof SetActionType) return
(o. toString(). equals( literalName)); else return( false);
}
}


