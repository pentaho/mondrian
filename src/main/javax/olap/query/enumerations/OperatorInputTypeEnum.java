package javax. olap. query. enumerations;
import java. util.*;
public class OperatorInputTypeEnum implements OperatorInputType {
public static final OperatorInputTypeEnum ATTRIBUTEREFERENCE = new OperatorInputTypeEnum(" ATTRIBUTEREFERENCE");
public static final OperatorInputTypeEnum MEMBERREFERENCE = new OperatorInputTypeEnum(" MEMBERREFERENCE");
public static final OperatorInputTypeEnum QUALIFIEDMEMBERREFERENCE = new OperatorInputTypeEnum(" QUALIFIEDMEMBERREFERENCE");
public static final OperatorInputTypeEnum DERIVEDATTRIBUTE = new OperatorInputTypeEnum(" DERIVEDATTRIBUTE");
public static final OperatorInputTypeEnum LITERALREFERENCE = new OperatorInputTypeEnum(" LITERALREFERENCE");
public static final OperatorInputTypeEnum OPERATORREFERENCE = new OperatorInputTypeEnum(" OPERATORREFERENCE");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" ATTRIBUTEREFERENCE");
temp. add(" MEMBERREFERENCE"); temp. add(" QUALIFIEDMEMBERREFERENCE");
temp. add(" DERIVEDATTRIBUTE"); temp. add(" LITERALREFERENCE");
temp. add(" OPERATORREFERENCE"); typeName = java. util. Collections. unmodifiableList( temp);
}
private OperatorInputTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof OperatorInputTypeEnum) return (o == this); else if( o instanceof OperatorInputType) return
(o. toString(). equals( literalName)); else return( false);
}
}


