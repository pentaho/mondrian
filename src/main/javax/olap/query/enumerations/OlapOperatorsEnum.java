package javax. olap. query. enumerations;
import java. util.*;
public class OlapOperatorsEnum implements OlapOperators {
public static final OlapOperatorsEnum LEAD = new OlapOperatorsEnum(" LEAD");
public static final OlapOperatorsEnum LAG = new OlapOperatorsEnum(" LAG");
public static final OlapOperatorsEnum SHARE = new OlapOperatorsEnum(" SHARE");
public static final OlapOperatorsEnum SHARETOPARENT = new OlapOperatorsEnum(" SHARETOPARENT");
public static final OlapOperatorsEnum SHARETOLEVEL = new OlapOperatorsEnum(" SHARETOLEVEL");
public static final OlapOperatorsEnum PRIORPERIOD = new OlapOperatorsEnum(" PRIORPERIOD");
public static final OlapOperatorsEnum SAMEELEMENTNANCESTORSAGO = new OlapOperatorsEnum(" SAMEELEMENTNANCESTORSAGO");
public static final OlapOperatorsEnum SAMEPERIODNANCESTORSAGO = new OlapOperatorsEnum(" SAMEPERIODNANCESTORSAGO");
public static final OlapOperatorsEnum PERIODTODATE = new OlapOperatorsEnum(" PERIODTODATE");
public static final OlapOperatorsEnum MOVINGSUM = new OlapOperatorsEnum(" MOVINGSUM");
public static final OlapOperatorsEnum MOVINGAVERAGE = new OlapOperatorsEnum(" MOVINGAVERAGE");
public static final OlapOperatorsEnum MOVINGMIN = new OlapOperatorsEnum(" MOVINGMIN");
public static final OlapOperatorsEnum MOVINGMAX = new OlapOperatorsEnum(" MOVINGMAX");
public static final OlapOperatorsEnum MOVINGCOUNT = new OlapOperatorsEnum(" MOVINGCOUNT");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" LEAD");
temp. add(" LAG"); temp. add(" SHARE");
temp. add(" SHARETOPARENT"); temp. add(" SHARETOLEVEL");
temp. add(" PRIORPERIOD"); temp. add(" SAMEELEMENTNANCESTORSAGO");
temp. add(" SAMEPERIODNANCESTORSAGO"); temp. add(" PERIODTODATE");
temp. add(" MOVINGSUM"); temp. add(" MOVINGAVERAGE");
temp. add(" MOVINGMIN"); temp. add(" MOVINGMAX");
temp. add(" MOVINGCOUNT"); typeName = java. util. Collections. unmodifiableList( temp);
}
private OlapOperatorsEnum( String literalName) {
this. literalName = literalName; }


public String toString()
{ return( literalName);
}


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof OlapOperatorsEnum) return (o == this); else if( o instanceof OlapOperators) return
(o. toString(). equals( literalName)); else return( false);
}
}


