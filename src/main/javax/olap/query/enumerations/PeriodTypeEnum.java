package javax. olap. query. enumerations;
import java. util.*;
public class PeriodTypeEnum implements PeriodType {
public static final PeriodTypeEnum YEAR = new PeriodTypeEnum(" YEAR");
public static final PeriodTypeEnum MONTH = new PeriodTypeEnum(" MONTH");
public static final PeriodTypeEnum QUARTER = new PeriodTypeEnum(" QUARTER");
public static final PeriodTypeEnum WEEK = new PeriodTypeEnum(" WEEK"); public static final PeriodTypeEnum DAY = new PeriodTypeEnum(" DAY");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" YEAR");
temp. add(" MONTH"); temp. add(" QUARTER");
temp. add(" WEEK"); temp. add(" DAY");
typeName = java. util. Collections. unmodifiableList( temp); }


private PeriodTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof PeriodTypeEnum) return (o == this); else if( o instanceof PeriodType) return
(o. toString(). equals( literalName)); else return( false);
}
}


