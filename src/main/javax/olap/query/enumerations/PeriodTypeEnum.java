/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class PeriodTypeEnum
implements javax.olap.query.enumerations.PeriodType {

  public static final javax.olap.query.enumerations.PeriodTypeEnum YEAR = new PeriodTypeEnum("Year");

  public static final javax.olap.query.enumerations.PeriodTypeEnum MONTH = new PeriodTypeEnum("Month");

  public static final javax.olap.query.enumerations.PeriodTypeEnum QUARTER = new PeriodTypeEnum("Quarter");

  public static final javax.olap.query.enumerations.PeriodTypeEnum WEEK = new PeriodTypeEnum("Week");

  public static final javax.olap.query.enumerations.PeriodTypeEnum DAY = new PeriodTypeEnum("Day");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","PeriodType"}));

  private final java.lang.String literalName;

  private PeriodTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.PeriodTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.PeriodType forName( java.lang.String value ) {
    if ( value.equals(YEAR.literalName) ) return YEAR;
    if ( value.equals(MONTH.literalName) ) return MONTH;
    if ( value.equals(QUARTER.literalName) ) return QUARTER;
    if ( value.equals(WEEK.literalName) ) return WEEK;
    if ( value.equals(DAY.literalName) ) return DAY;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.PeriodType'");
  }

}
