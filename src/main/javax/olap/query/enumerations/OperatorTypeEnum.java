package javax. olap. query. enumerations;
import java. util.*;
public class OperatorTypeEnum implements OperatorType {
public static final OperatorTypeEnum GT = new OperatorTypeEnum(" GT"); public static final OperatorTypeEnum LE = new OperatorTypeEnum(" LE");
public static final OperatorTypeEnum EQ = new OperatorTypeEnum(" EQ"); public static final OperatorTypeEnum NE = new OperatorTypeEnum(" NE");
public static final OperatorTypeEnum GE = new OperatorTypeEnum(" GE"); public static final OperatorTypeEnum LT = new OperatorTypeEnum(" LT");
private static final List typeName; private final String literalName;
static { java. util. ArrayList temp = new java. util. ArrayList();
temp. add(" GT"); temp. add(" LE");
temp. add(" EQ"); temp. add(" NE");
temp. add(" GE"); temp. add(" LT");
typeName = java. util. Collections. unmodifiableList( temp); }


private OperatorTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof OperatorTypeEnum) return (o == this); else if( o instanceof OperatorType) return
(o. toString(). equals( literalName)); else return( false);
}
}


