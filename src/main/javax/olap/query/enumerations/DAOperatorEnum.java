package javax. olap. query. enumerations;
import java. util.*;
public class DAOperatorEnum implements DAOperator {
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); typeName = java. util. Collections. unmodifiableList( temp);
}
private DAOperatorEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof DAOperatorEnum) return (o == this); else if( o instanceof DAOperator) return
(o. toString(). equals( literalName)); else return( false);
}
}


