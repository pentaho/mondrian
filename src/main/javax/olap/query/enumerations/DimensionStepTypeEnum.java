/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class DimensionStepTypeEnum
implements javax.olap.query.enumerations.DimensionStepType {

  public static final javax.olap.query.enumerations.DimensionStepTypeEnum MEMBER_LIST_FILTER = new DimensionStepTypeEnum("MemberListFilter");

  public static final javax.olap.query.enumerations.DimensionStepTypeEnum SINGLE_MEMBER_FILTER = new DimensionStepTypeEnum("SingleMemberFilter");

  public static final javax.olap.query.enumerations.DimensionStepTypeEnum ATTRIBUTE_FILTER = new DimensionStepTypeEnum("AttributeFilter");

  public static final javax.olap.query.enumerations.DimensionStepTypeEnum LEVEL_FILTER = new DimensionStepTypeEnum("LevelFilter");

  public static final javax.olap.query.enumerations.DimensionStepTypeEnum DRILL_FILTER = new DimensionStepTypeEnum("DrillFilter");

  public static final javax.olap.query.enumerations.DimensionStepTypeEnum HIERARCHY_MEMBER_FILTER = new DimensionStepTypeEnum("HierarchyMemberFilter");

  public static final javax.olap.query.enumerations.DimensionStepTypeEnum ATTRIBUTE_SORT = new DimensionStepTypeEnum("AttributeSort");

  public static final javax.olap.query.enumerations.DimensionStepTypeEnum HIERARCHICAL_SORT = new DimensionStepTypeEnum("HierarchicalSort");

  public static final javax.olap.query.enumerations.DimensionStepTypeEnum DATA_BASED_SORT = new DimensionStepTypeEnum("DataBasedSort");

  public static final javax.olap.query.enumerations.DimensionStepTypeEnum COMPOUND_DIMENSION_STEP = new DimensionStepTypeEnum("CompoundDimensionStep");

  public static final javax.olap.query.enumerations.DimensionStepTypeEnum EXCEPTION_MEMBER_FILTER = new DimensionStepTypeEnum("ExceptionMemberFilter");

  public static final javax.olap.query.enumerations.DimensionStepTypeEnum RANKING_MEMBER_FILTER = new DimensionStepTypeEnum("RankingMemberFilter");

  public static final javax.olap.query.enumerations.DimensionStepTypeEnum DERIVED_ATTRIBUTE_FILTER = new DimensionStepTypeEnum("DerivedAttributeFilter");

  public static final javax.olap.query.enumerations.DimensionStepTypeEnum DERIVED_ATTRIBUTE_SORT = new DimensionStepTypeEnum("DerivedAttributeSort");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","DimensionStepType"}));

  private final java.lang.String literalName;

  private DimensionStepTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.DimensionStepTypeEnum)?other == this:
      ((other instanceof javax.jmi.reflect.RefEnum) && ((javax.jmi.reflect.RefEnum)other).refTypeName().equals(typeName) && ((javax.jmi.reflect.RefEnum)other).toString().equals(literalName));
  }

  protected java.lang.Object readResolve()
    throws java.io.InvalidObjectException {
    try {
      return forName(literalName);
    } catch ( java.lang.IllegalArgumentException iae ) {
      throw new java.io.InvalidObjectException(iae.getMessage());
    }
  }

  public int hashCode() {
    return literalName.hashCode();
  }

  public static javax.olap.query.enumerations.DimensionStepType forName( java.lang.String value ) {
    if ( value.equals(MEMBER_LIST_FILTER.literalName) ) return MEMBER_LIST_FILTER;
    if ( value.equals(SINGLE_MEMBER_FILTER.literalName) ) return SINGLE_MEMBER_FILTER;
    if ( value.equals(ATTRIBUTE_FILTER.literalName) ) return ATTRIBUTE_FILTER;
    if ( value.equals(LEVEL_FILTER.literalName) ) return LEVEL_FILTER;
    if ( value.equals(DRILL_FILTER.literalName) ) return DRILL_FILTER;
    if ( value.equals(HIERARCHY_MEMBER_FILTER.literalName) ) return HIERARCHY_MEMBER_FILTER;
    if ( value.equals(ATTRIBUTE_SORT.literalName) ) return ATTRIBUTE_SORT;
    if ( value.equals(HIERARCHICAL_SORT.literalName) ) return HIERARCHICAL_SORT;
    if ( value.equals(DATA_BASED_SORT.literalName) ) return DATA_BASED_SORT;
    if ( value.equals(COMPOUND_DIMENSION_STEP.literalName) ) return COMPOUND_DIMENSION_STEP;
    if ( value.equals(EXCEPTION_MEMBER_FILTER.literalName) ) return EXCEPTION_MEMBER_FILTER;
    if ( value.equals(RANKING_MEMBER_FILTER.literalName) ) return RANKING_MEMBER_FILTER;
    if ( value.equals(DERIVED_ATTRIBUTE_FILTER.literalName) ) return DERIVED_ATTRIBUTE_FILTER;
    if ( value.equals(DERIVED_ATTRIBUTE_SORT.literalName) ) return DERIVED_ATTRIBUTE_SORT;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.DimensionStepType'");
  }

}
