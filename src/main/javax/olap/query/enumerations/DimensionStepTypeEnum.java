package javax. olap. query. enumerations;
import java. util.*;
public class DimensionStepTypeEnum implements DimensionStepType {
public static final DimensionStepTypeEnum MEMBERLISTFILTER = new DimensionStepTypeEnum(" MEMBERLISTFILTER");
public static final DimensionStepTypeEnum SINGLEMEMBERFILTER = new DimensionStepTypeEnum(" SINGLEMEMBERFILTER");
public static final DimensionStepTypeEnum ATTRIBUTEFILTER = new DimensionStepTypeEnum(" ATTRIBUTEFILTER");
public static final DimensionStepTypeEnum LEVELFILTER = new DimensionStepTypeEnum(" LEVELFILTER");
public static final DimensionStepTypeEnum DRILLFILTER = new DimensionStepTypeEnum(" DRILLFILTER");
public static final DimensionStepTypeEnum HIERARCHYFILTER = new DimensionStepTypeEnum(" HIERARCHYFILTER");
public static final DimensionStepTypeEnum DATABASEDMEMBERFILTER = new DimensionStepTypeEnum(" DATABASEDMEMBERFILTER");
public static final DimensionStepTypeEnum ATTRIBUTESORT = new DimensionStepTypeEnum(" ATTRIBUTESORT");
public static final DimensionStepTypeEnum HIERARCHICALSORT = new DimensionStepTypeEnum(" HIERARCHICALSORT");
public static final DimensionStepTypeEnum DATABASEDSORT = new DimensionStepTypeEnum(" DATABASEDSORT");
public static final DimensionStepTypeEnum COMPOUNDDIMENSIONSTEP = new DimensionStepTypeEnum(" COMPOUNDDIMENSIONSTEP");
public static final DimensionStepTypeEnum EXCEPTIONMEMBERFILTER = new DimensionStepTypeEnum(" EXCEPTIONMEMBERFILTER");
public static final DimensionStepTypeEnum RANKINGMEMBERFILTER = new DimensionStepTypeEnum(" RANKINGMEMBERFILTER");
private static final List typeName; private final String literalName;
static {
java. util. ArrayList temp = new java. util. ArrayList(); temp. add(" MEMBERLISTFILTER");
temp. add(" SINGLEMEMBERFILTER"); temp. add(" ATTRIBUTEFILTER");
temp. add(" LEVELFILTER"); temp. add(" DRILLFILTER");
temp. add(" HIERARCHYFILTER");
temp. add(" DATABASEDMEMBERFILTER"); temp. add(" ATTRIBUTESORT");
temp. add(" HIERARCHICALSORT"); temp. add(" DATABASEDSORT");
temp. add(" COMPOUNDDIMENSIONSTEP"); temp. add(" EXCEPTIONMEMBERFILTER");
temp. add(" RANKINGMEMBERFILTER"); typeName = java. util. Collections. unmodifiableList( temp);
}
private DimensionStepTypeEnum( String literalName) {
this. literalName = literalName; }


public String toString() {
return( literalName); }


public List refTypeName() {
return( typeName); }


public int hashCode() {
return( literalName. hashCode()); }


public boolean equals( Object o) {
if( o instanceof DimensionStepTypeEnum) return (o == this); else if( o instanceof DimensionStepType) return
(o. toString(). equals( literalName)); else return( false);
}
}


