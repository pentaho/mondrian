package org. omg. cwm. objectmodel. behavioral;
import java. util.*;
public class ParameterDirectionKindEnum implements ParameterDirectionKind
{
public static final ParameterDirectionKindEnum PDK_IN = new ParameterDirectionKindEnum("PDK_IN");
public static final ParameterDirectionKindEnum PDK_INOUT = new ParameterDirectionKindEnum("PDK_INOUT");
public static final ParameterDirectionKindEnum PDK_OUT = new ParameterDirectionKindEnum("PDK_OUT");
public static final ParameterDirectionKindEnum PDK_RETURN = new ParameterDirectionKindEnum("PDK_RETURN");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add("PDK_IN");
temp. add("PDK_INOUT"); temp. add("PDK_OUT");
temp. add("PDK_RETURN"); typeName = java. util. Collections. unmodifiableList( temp);
}
private ParameterDirectionKindEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof ParameterDirectionKindEnum) return (o == this); else if( o instanceof ParameterDirectionKind) return
(o. toString(). equals( literalName)); else return( false);
}
}


