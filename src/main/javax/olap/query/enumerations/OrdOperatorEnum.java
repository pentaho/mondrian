/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class OrdOperatorEnum
implements javax.olap.query.enumerations.OrdOperator {

  public static final javax.olap.query.enumerations.OrdOperatorEnum SUM = new OrdOperatorEnum("Sum");

  public static final javax.olap.query.enumerations.OrdOperatorEnum COUNT = new OrdOperatorEnum("Count");

  public static final javax.olap.query.enumerations.OrdOperatorEnum AVERAGE = new OrdOperatorEnum("Average");

  public static final javax.olap.query.enumerations.OrdOperatorEnum WEIGHTED_AVERAGE = new OrdOperatorEnum("WeightedAverage");

  public static final javax.olap.query.enumerations.OrdOperatorEnum MAXIMUM = new OrdOperatorEnum("Maximum");

  public static final javax.olap.query.enumerations.OrdOperatorEnum MINIMUM = new OrdOperatorEnum("Minimum");

  public static final javax.olap.query.enumerations.OrdOperatorEnum STANDARD_DEVIATION = new OrdOperatorEnum("StandardDeviation");

  public static final javax.olap.query.enumerations.OrdOperatorEnum FIRST = new OrdOperatorEnum("First");

  public static final javax.olap.query.enumerations.OrdOperatorEnum LAST = new OrdOperatorEnum("Last");

  public static final javax.olap.query.enumerations.OrdOperatorEnum MEAN = new OrdOperatorEnum("Mean");

  public static final javax.olap.query.enumerations.OrdOperatorEnum MODE = new OrdOperatorEnum("Mode");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","OrdOperator"}));

  private final java.lang.String literalName;

  private OrdOperatorEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.OrdOperatorEnum)?other == this:
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

  public static javax.olap.query.enumerations.OrdOperator forName( java.lang.String value ) {
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
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.OrdOperator'");
  }

}
