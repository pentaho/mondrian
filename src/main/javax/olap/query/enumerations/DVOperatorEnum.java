/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class DVOperatorEnum
implements javax.olap.query.enumerations.DVOperator {

  public static final javax.olap.query.enumerations.DVOperatorEnum SUM = new DVOperatorEnum("Sum");

  public static final javax.olap.query.enumerations.DVOperatorEnum COUNT = new DVOperatorEnum("Count");

  public static final javax.olap.query.enumerations.DVOperatorEnum AVERAGE = new DVOperatorEnum("Average");

  public static final javax.olap.query.enumerations.DVOperatorEnum WEIGHTED_AVERAGE = new DVOperatorEnum("WeightedAverage");

  public static final javax.olap.query.enumerations.DVOperatorEnum MAXIMUM = new DVOperatorEnum("Maximum");

  public static final javax.olap.query.enumerations.DVOperatorEnum MINIMUM = new DVOperatorEnum("Minimum");

  public static final javax.olap.query.enumerations.DVOperatorEnum STANDARD_DEVIATION = new DVOperatorEnum("StandardDeviation");

  public static final javax.olap.query.enumerations.DVOperatorEnum FIRST = new DVOperatorEnum("First");

  public static final javax.olap.query.enumerations.DVOperatorEnum LAST = new DVOperatorEnum("Last");

  public static final javax.olap.query.enumerations.DVOperatorEnum MEAN = new DVOperatorEnum("Mean");

  public static final javax.olap.query.enumerations.DVOperatorEnum MODE = new DVOperatorEnum("Mode");

  public static final javax.olap.query.enumerations.DVOperatorEnum LEAD = new DVOperatorEnum("Lead");

  public static final javax.olap.query.enumerations.DVOperatorEnum LAG = new DVOperatorEnum("Lag");

  public static final javax.olap.query.enumerations.DVOperatorEnum SHARE = new DVOperatorEnum("Share");

  public static final javax.olap.query.enumerations.DVOperatorEnum SHARE_TO_PARENT = new DVOperatorEnum("ShareToParent");

  public static final javax.olap.query.enumerations.DVOperatorEnum SHARE_TO_LEVEL = new DVOperatorEnum("ShareToLevel");

  public static final javax.olap.query.enumerations.DVOperatorEnum PRIOR_PERIOD = new DVOperatorEnum("PriorPeriod");

  public static final javax.olap.query.enumerations.DVOperatorEnum SAME_ELEMENT_NANCESTORS_AGO = new DVOperatorEnum("SameElementNAncestorsAgo");

  public static final javax.olap.query.enumerations.DVOperatorEnum SAME_PERIOD_NANCESTORS_AGO = new DVOperatorEnum("SamePeriodNAncestorsAgo");

  public static final javax.olap.query.enumerations.DVOperatorEnum PERIOD_TO_DATE = new DVOperatorEnum("PeriodToDate");

  public static final javax.olap.query.enumerations.DVOperatorEnum MOVING_SUM = new DVOperatorEnum("MovingSum");

  public static final javax.olap.query.enumerations.DVOperatorEnum MOVING_AVERAGE = new DVOperatorEnum("MovingAverage");

  public static final javax.olap.query.enumerations.DVOperatorEnum MOVING_MIN = new DVOperatorEnum("MovingMin");

  public static final javax.olap.query.enumerations.DVOperatorEnum MOVING_MAX = new DVOperatorEnum("MovingMax");

  public static final javax.olap.query.enumerations.DVOperatorEnum MOVING_COUNT = new DVOperatorEnum("MovingCount");

  public static final javax.olap.query.enumerations.DVOperatorEnum ABSOLUTE_VALUE = new DVOperatorEnum("AbsoluteValue");

  public static final javax.olap.query.enumerations.DVOperatorEnum ADD = new DVOperatorEnum("Add");

  public static final javax.olap.query.enumerations.DVOperatorEnum CEILING = new DVOperatorEnum("Ceiling");

  public static final javax.olap.query.enumerations.DVOperatorEnum COSINE = new DVOperatorEnum("Cosine");

  public static final javax.olap.query.enumerations.DVOperatorEnum DIVIDE = new DVOperatorEnum("Divide");

  public static final javax.olap.query.enumerations.DVOperatorEnum EXP = new DVOperatorEnum("Exp");

  public static final javax.olap.query.enumerations.DVOperatorEnum FLOOR = new DVOperatorEnum("Floor");

  public static final javax.olap.query.enumerations.DVOperatorEnum LOG = new DVOperatorEnum("Log");

  public static final javax.olap.query.enumerations.DVOperatorEnum MULTIPLY = new DVOperatorEnum("Multiply");

  public static final javax.olap.query.enumerations.DVOperatorEnum POWER = new DVOperatorEnum("Power");

  public static final javax.olap.query.enumerations.DVOperatorEnum ROUND = new DVOperatorEnum("Round");

  public static final javax.olap.query.enumerations.DVOperatorEnum REMAINDER = new DVOperatorEnum("Remainder");

  public static final javax.olap.query.enumerations.DVOperatorEnum SINE = new DVOperatorEnum("Sine");

  public static final javax.olap.query.enumerations.DVOperatorEnum SUBTRACT = new DVOperatorEnum("Subtract");

  public static final javax.olap.query.enumerations.DVOperatorEnum SQUARE_ROOT = new DVOperatorEnum("SquareRoot");

  public static final javax.olap.query.enumerations.DVOperatorEnum TANGENT = new DVOperatorEnum("Tangent");

  public static final javax.olap.query.enumerations.DVOperatorEnum UNARY_NEGATION = new DVOperatorEnum("UnaryNegation");

  public static final javax.olap.query.enumerations.DVOperatorEnum CONCATENATION = new DVOperatorEnum("Concatenation");

  public static final javax.olap.query.enumerations.DVOperatorEnum SUB_STRING = new DVOperatorEnum("SubString");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","DVOperator"}));

  private final java.lang.String literalName;

  private DVOperatorEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.DVOperatorEnum)?other == this:
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

  public static javax.olap.query.enumerations.DVOperator forName( java.lang.String value ) {
    if ( value.equals(SUM.literalName) ) return SUM;
    if ( value.equals(COUNT.literalName) ) return COUNT;
    if ( value.equals(AVERAGE.literalName) ) return AVERAGE;
    if ( value.equals(WEIGHTED_AVERAGE.literalName) ) return WEIGHTED_AVERAGE;
    if ( value.equals(MAXIMUM.literalName) ) return MAXIMUM;
    if ( value.equals(MINIMUM.literalName) ) return MINIMUM;
    if ( value.equals(STANDARD_DEVIATION.literalName) ) return STANDARD_DEVIATION;
    if ( value.equals(FIRST.literalName) ) return FIRST;
    if ( value.equals(LAST.literalName) ) return LAST;
    if ( value.equals(MEAN.literalName) ) return MEAN;
    if ( value.equals(MODE.literalName) ) return MODE;
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
    if ( value.equals(ABSOLUTE_VALUE.literalName) ) return ABSOLUTE_VALUE;
    if ( value.equals(ADD.literalName) ) return ADD;
    if ( value.equals(CEILING.literalName) ) return CEILING;
    if ( value.equals(COSINE.literalName) ) return COSINE;
    if ( value.equals(DIVIDE.literalName) ) return DIVIDE;
    if ( value.equals(EXP.literalName) ) return EXP;
    if ( value.equals(FLOOR.literalName) ) return FLOOR;
    if ( value.equals(LOG.literalName) ) return LOG;
    if ( value.equals(MULTIPLY.literalName) ) return MULTIPLY;
    if ( value.equals(POWER.literalName) ) return POWER;
    if ( value.equals(ROUND.literalName) ) return ROUND;
    if ( value.equals(REMAINDER.literalName) ) return REMAINDER;
    if ( value.equals(SINE.literalName) ) return SINE;
    if ( value.equals(SUBTRACT.literalName) ) return SUBTRACT;
    if ( value.equals(SQUARE_ROOT.literalName) ) return SQUARE_ROOT;
    if ( value.equals(TANGENT.literalName) ) return TANGENT;
    if ( value.equals(UNARY_NEGATION.literalName) ) return UNARY_NEGATION;
    if ( value.equals(CONCATENATION.literalName) ) return CONCATENATION;
    if ( value.equals(SUB_STRING.literalName) ) return SUB_STRING;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.DVOperator'");
  }

}
