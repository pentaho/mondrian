/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class DAOperatorEnum
implements javax.olap.query.enumerations.DAOperator {

  public static final javax.olap.query.enumerations.DAOperatorEnum ABSOLUTE_VALUE = new DAOperatorEnum("AbsoluteValue");

  public static final javax.olap.query.enumerations.DAOperatorEnum ADD = new DAOperatorEnum("Add");

  public static final javax.olap.query.enumerations.DAOperatorEnum CEILING = new DAOperatorEnum("Ceiling");

  public static final javax.olap.query.enumerations.DAOperatorEnum COSINE = new DAOperatorEnum("Cosine");

  public static final javax.olap.query.enumerations.DAOperatorEnum DIVIDE = new DAOperatorEnum("Divide");

  public static final javax.olap.query.enumerations.DAOperatorEnum EXP = new DAOperatorEnum("Exp");

  public static final javax.olap.query.enumerations.DAOperatorEnum FLOOR = new DAOperatorEnum("Floor");

  public static final javax.olap.query.enumerations.DAOperatorEnum LOG = new DAOperatorEnum("Log");

  public static final javax.olap.query.enumerations.DAOperatorEnum MULTIPLY = new DAOperatorEnum("Multiply");

  public static final javax.olap.query.enumerations.DAOperatorEnum POWER = new DAOperatorEnum("Power");

  public static final javax.olap.query.enumerations.DAOperatorEnum ROUND = new DAOperatorEnum("Round");

  public static final javax.olap.query.enumerations.DAOperatorEnum REMAINDER = new DAOperatorEnum("Remainder");

  public static final javax.olap.query.enumerations.DAOperatorEnum SINE = new DAOperatorEnum("Sine");

  public static final javax.olap.query.enumerations.DAOperatorEnum SUBTRACT = new DAOperatorEnum("Subtract");

  public static final javax.olap.query.enumerations.DAOperatorEnum SQUARE_ROOT = new DAOperatorEnum("SquareRoot");

  public static final javax.olap.query.enumerations.DAOperatorEnum TANGENT = new DAOperatorEnum("Tangent");

  public static final javax.olap.query.enumerations.DAOperatorEnum UNARY_NEGATION = new DAOperatorEnum("UnaryNegation");

  public static final javax.olap.query.enumerations.DAOperatorEnum CONCATENATION = new DAOperatorEnum("Concatenation");

  public static final javax.olap.query.enumerations.DAOperatorEnum SUB_STRING = new DAOperatorEnum("SubString");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","DAOperator"}));

  private final java.lang.String literalName;

  private DAOperatorEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.DAOperatorEnum)?other == this:
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

  public static javax.olap.query.enumerations.DAOperator forName( java.lang.String value ) {
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
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.DAOperator'");
  }

}
