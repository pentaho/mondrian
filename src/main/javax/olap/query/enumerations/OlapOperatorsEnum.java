/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class OlapOperatorsEnum
implements javax.olap.query.enumerations.OlapOperators {

  public static final javax.olap.query.enumerations.OlapOperatorsEnum LEAD = new OlapOperatorsEnum("Lead");

  public static final javax.olap.query.enumerations.OlapOperatorsEnum LAG = new OlapOperatorsEnum("Lag");

  public static final javax.olap.query.enumerations.OlapOperatorsEnum SHARE = new OlapOperatorsEnum("Share");

  public static final javax.olap.query.enumerations.OlapOperatorsEnum SHARE_TO_PARENT = new OlapOperatorsEnum("ShareToParent");

  public static final javax.olap.query.enumerations.OlapOperatorsEnum SHARE_TO_LEVEL = new OlapOperatorsEnum("ShareToLevel");

  public static final javax.olap.query.enumerations.OlapOperatorsEnum PRIOR_PERIOD = new OlapOperatorsEnum("PriorPeriod");

  public static final javax.olap.query.enumerations.OlapOperatorsEnum SAME_ELEMENT_NANCESTORS_AGO = new OlapOperatorsEnum("SameElementNAncestorsAgo");

  public static final javax.olap.query.enumerations.OlapOperatorsEnum SAME_PERIOD_NANCESTORS_AGO = new OlapOperatorsEnum("SamePeriodNAncestorsAgo");

  public static final javax.olap.query.enumerations.OlapOperatorsEnum PERIOD_TO_DATE = new OlapOperatorsEnum("PeriodToDate");

  public static final javax.olap.query.enumerations.OlapOperatorsEnum MOVING_SUM = new OlapOperatorsEnum("MovingSum");

  public static final javax.olap.query.enumerations.OlapOperatorsEnum MOVING_AVERAGE = new OlapOperatorsEnum("MovingAverage");

  public static final javax.olap.query.enumerations.OlapOperatorsEnum MOVING_MIN = new OlapOperatorsEnum("MovingMin");

  public static final javax.olap.query.enumerations.OlapOperatorsEnum MOVING_MAX = new OlapOperatorsEnum("MovingMax");

  public static final javax.olap.query.enumerations.OlapOperatorsEnum MOVING_COUNT = new OlapOperatorsEnum("MovingCount");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","OlapOperators"}));

  private final java.lang.String literalName;

  private OlapOperatorsEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.OlapOperatorsEnum)?other == this:
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

  public static javax.olap.query.enumerations.OlapOperators forName( java.lang.String value ) {
    if ( value.equals(LEAD.literalName) ) return LEAD;
    if ( value.equals(LAG.literalName) ) return LAG;
    if ( value.equals(SHARE.literalName) ) return SHARE;
    if ( value.equals(SHARE_TO_PARENT.literalName) ) return SHARE_TO_PARENT;
    if ( value.equals(SHARE_TO_LEVEL.literalName) ) return SHARE_TO_LEVEL;
    if ( value.equals(PRIOR_PERIOD.literalName) ) return PRIOR_PERIOD;
    if ( value.equals(SAME_ELEMENT_NANCESTORS_AGO.literalName) ) return SAME_ELEMENT_NANCESTORS_AGO;
    if ( value.equals(SAME_PERIOD_NANCESTORS_AGO.literalName) ) return SAME_PERIOD_NANCESTORS_AGO;
    if ( value.equals(PERIOD_TO_DATE.literalName) ) return PERIOD_TO_DATE;
    if ( value.equals(MOVING_SUM.literalName) ) return MOVING_SUM;
    if ( value.equals(MOVING_AVERAGE.literalName) ) return MOVING_AVERAGE;
    if ( value.equals(MOVING_MIN.literalName) ) return MOVING_MIN;
    if ( value.equals(MOVING_MAX.literalName) ) return MOVING_MAX;
    if ( value.equals(MOVING_COUNT.literalName) ) return MOVING_COUNT;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.OlapOperators'");
  }

}
