/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class SetOrientedOperatorsEnum
implements javax.olap.query.enumerations.SetOrientedOperators {

  public static final javax.olap.query.enumerations.SetOrientedOperatorsEnum SUM = new SetOrientedOperatorsEnum("Sum");

  public static final javax.olap.query.enumerations.SetOrientedOperatorsEnum COUNT = new SetOrientedOperatorsEnum("Count");

  public static final javax.olap.query.enumerations.SetOrientedOperatorsEnum AVERAGE = new SetOrientedOperatorsEnum("Average");

  public static final javax.olap.query.enumerations.SetOrientedOperatorsEnum WEIGHTED_AVERAGE = new SetOrientedOperatorsEnum("WeightedAverage");

  public static final javax.olap.query.enumerations.SetOrientedOperatorsEnum MAXIMUM = new SetOrientedOperatorsEnum("Maximum");

  public static final javax.olap.query.enumerations.SetOrientedOperatorsEnum MINIMUM = new SetOrientedOperatorsEnum("Minimum");

  public static final javax.olap.query.enumerations.SetOrientedOperatorsEnum STANDARD_DEVIATION = new SetOrientedOperatorsEnum("StandardDeviation");

  public static final javax.olap.query.enumerations.SetOrientedOperatorsEnum FIRST = new SetOrientedOperatorsEnum("First");

  public static final javax.olap.query.enumerations.SetOrientedOperatorsEnum LAST = new SetOrientedOperatorsEnum("Last");

  public static final javax.olap.query.enumerations.SetOrientedOperatorsEnum MEAN = new SetOrientedOperatorsEnum("Mean");

  public static final javax.olap.query.enumerations.SetOrientedOperatorsEnum MODE = new SetOrientedOperatorsEnum("Mode");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","SetOrientedOperators"}));

  private final java.lang.String literalName;

  private SetOrientedOperatorsEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.SetOrientedOperatorsEnum)?other == this:
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

  public static javax.olap.query.enumerations.SetOrientedOperators forName( java.lang.String value ) {
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
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.SetOrientedOperators'");
  }

}
