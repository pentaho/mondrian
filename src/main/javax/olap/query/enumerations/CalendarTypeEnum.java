package javax. olap. query. enumerations;
import java. util.*;
public class CalendarTypeEnum implements CalendarType {
public static final CalendarTypeEnum GREGORIAN = new CalendarTypeEnum("GREGORIAN");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add("GREGORIAN");
typeName = java. util. Collections. unmodifiableList( temp);
}
private CalendarTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof CalendarTypeEnum) return (o == this); else if( o instanceof CalendarType) return
(o. toString(). equals( literalName)); else return( false);
}
}


