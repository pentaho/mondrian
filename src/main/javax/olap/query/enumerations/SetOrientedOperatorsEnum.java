package javax. olap. query. enumerations;
import java. util.*;
public class SetOrientedOperatorsEnum implements SetOrientedOperators {
public static final SetOrientedOperatorsEnum SUM = new SetOrientedOperatorsEnum(" SUM");
public static final SetOrientedOperatorsEnum COUNT = new SetOrientedOperatorsEnum(" COUNT");
public static final SetOrientedOperatorsEnum AVERAGE = new SetOrientedOperatorsEnum(" AVERAGE");
public static final SetOrientedOperatorsEnum WEIGHTEDAVERAGE = new SetOrientedOperatorsEnum(" WEIGHTEDAVERAGE");
public static final SetOrientedOperatorsEnum MAXIMUM = new SetOrientedOperatorsEnum(" MAXIMUM");
public static final SetOrientedOperatorsEnum MINIMUM = new SetOrientedOperatorsEnum(" MINIMUM");
public static final SetOrientedOperatorsEnum STANDARDDEVIATION = new SetOrientedOperatorsEnum(" STANDARDDEVIATION");
public static final SetOrientedOperatorsEnum FIRST = new SetOrientedOperatorsEnum(" FIRST");
public static final SetOrientedOperatorsEnum LAST = new SetOrientedOperatorsEnum(" LAST");
public static final SetOrientedOperatorsEnum MEAN = new SetOrientedOperatorsEnum(" MEAN");
public static final SetOrientedOperatorsEnum MODE = new SetOrientedOperatorsEnum(" MODE");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" SUM");
temp. add(" COUNT"); temp. add(" AVERAGE");
temp. add(" WEIGHTEDAVERAGE"); temp. add(" MAXIMUM");
temp. add(" MINIMUM"); temp. add(" STANDARDDEVIATION");
temp. add(" FIRST"); temp. add(" LAST");
temp. add(" MEAN"); temp. add(" MODE");
typeName = java. util. Collections. unmodifiableList( temp); }


private SetOrientedOperatorsEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }
public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof SetOrientedOperatorsEnum) return (o == this); else if( o instanceof SetOrientedOperators) return
(o. toString(). equals( literalName)); else return( false);
}
}


