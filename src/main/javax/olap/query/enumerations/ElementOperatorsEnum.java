package javax. olap. query. enumerations;
import java. util.*;
public class ElementOperatorsEnum implements ElementOperators {
public static final ElementOperatorsEnum ABSOLUTEVALUE = new ElementOperatorsEnum("ABSOLUTEVALUE");
public static final ElementOperatorsEnum ADD = new ElementOperatorsEnum("ADD");
public static final ElementOperatorsEnum CELING = new ElementOperatorsEnum("CELING");
public static final ElementOperatorsEnum COSINE = new ElementOperatorsEnum("COSINE");
public static final ElementOperatorsEnum DIVIDE = new ElementOperatorsEnum("DIVIDE");
public static final ElementOperatorsEnum EXP = new ElementOperatorsEnum("EXP");
public static final ElementOperatorsEnum FLOOR = new ElementOperatorsEnum("FLOOR");
public static final ElementOperatorsEnum LOG = new ElementOperatorsEnum("LOG");
public static final ElementOperatorsEnum MULTIPLY = new ElementOperatorsEnum("MULTIPLY");
public static final ElementOperatorsEnum POWER = new ElementOperatorsEnum("POWER");
public static final ElementOperatorsEnum ROUND = new ElementOperatorsEnum("ROUND");
public static final ElementOperatorsEnum REMAINDER = new ElementOperatorsEnum("REMAINDER");
public static final ElementOperatorsEnum SINE = new ElementOperatorsEnum("SINE");
public static final ElementOperatorsEnum SUBTRACT = new ElementOperatorsEnum("SUBTRACT");
public static final ElementOperatorsEnum SQUAREROOT = new ElementOperatorsEnum("SQUAREROOT");
public static final ElementOperatorsEnum TANGENT = new ElementOperatorsEnum("TANGENT");
public static final ElementOperatorsEnum UNARYNEGATION = new ElementOperatorsEnum("UNARYNEGATION");
public static final ElementOperatorsEnum CONCATENATION = new ElementOperatorsEnum("CONCATENATION");
public static final ElementOperatorsEnum SUBSTRING = new ElementOperatorsEnum("SUBSTRING");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add("ABSOLUTEVALUE");
temp. add("ADD"); temp. add("CELING");
temp. add("COSINE"); temp. add("DIVIDE");
temp. add("EXP"); temp. add("FLOOR");
temp. add("LOG"); temp. add("MULTIPLY");
temp. add("POWER"); temp. add("ROUND");
temp. add("REMAINDER"); temp. add("SINE");
temp. add("SUBTRACT"); temp. add("SQUAREROOT");
temp. add("TANGENT"); temp. add("UNARYNEGATION");
temp. add("CONCATENATION"); temp. add("SUBSTRING");
typeName = java. util. Collections. unmodifiableList( temp); }


private ElementOperatorsEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName);
}
public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof ElementOperatorsEnum)
	return (o == this);
else if( o instanceof ElementOperators)
	return(o. toString(). equals( literalName));
else
	return( false);
}
}


