/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class OperatorTypeEnum
implements javax.olap.query.enumerations.OperatorType {

  public static final javax.olap.query.enumerations.OperatorTypeEnum GT = new OperatorTypeEnum("GT");

  public static final javax.olap.query.enumerations.OperatorTypeEnum LE = new OperatorTypeEnum("LE");

  public static final javax.olap.query.enumerations.OperatorTypeEnum EQ = new OperatorTypeEnum("EQ");

  public static final javax.olap.query.enumerations.OperatorTypeEnum NE = new OperatorTypeEnum("NE");

  public static final javax.olap.query.enumerations.OperatorTypeEnum GE = new OperatorTypeEnum("GE");

  public static final javax.olap.query.enumerations.OperatorTypeEnum LT = new OperatorTypeEnum("LT");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","OperatorType"}));

  private final java.lang.String literalName;

  private OperatorTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.OperatorTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.OperatorType forName( java.lang.String value ) {
    if ( value.equals(GT.literalName) ) return GT;
    if ( value.equals(LE.literalName) ) return LE;
    if ( value.equals(EQ.literalName) ) return EQ;
    if ( value.equals(NE.literalName) ) return NE;
    if ( value.equals(GE.literalName) ) return GE;
    if ( value.equals(LT.literalName) ) return LT;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.OperatorType'");
  }

}
