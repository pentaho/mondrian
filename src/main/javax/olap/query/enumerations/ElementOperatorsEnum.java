/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class ElementOperatorsEnum
implements javax.olap.query.enumerations.ElementOperators {

  public static final javax.olap.query.enumerations.ElementOperatorsEnum ABSOLUTE_VALUE = new ElementOperatorsEnum("AbsoluteValue");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum ADD = new ElementOperatorsEnum("Add");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum CEILING = new ElementOperatorsEnum("Ceiling");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum COSINE = new ElementOperatorsEnum("Cosine");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum DIVIDE = new ElementOperatorsEnum("Divide");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum EXP = new ElementOperatorsEnum("Exp");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum FLOOR = new ElementOperatorsEnum("Floor");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum LOG = new ElementOperatorsEnum("Log");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum MULTIPLY = new ElementOperatorsEnum("Multiply");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum POWER = new ElementOperatorsEnum("Power");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum ROUND = new ElementOperatorsEnum("Round");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum REMAINDER = new ElementOperatorsEnum("Remainder");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum SINE = new ElementOperatorsEnum("Sine");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum SUBTRACT = new ElementOperatorsEnum("Subtract");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum SQUARE_ROOT = new ElementOperatorsEnum("SquareRoot");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum TANGENT = new ElementOperatorsEnum("Tangent");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum UNARY_NEGATION = new ElementOperatorsEnum("UnaryNegation");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum CONCATENATION = new ElementOperatorsEnum("Concatenation");

  public static final javax.olap.query.enumerations.ElementOperatorsEnum SUB_STRING = new ElementOperatorsEnum("SubString");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","ElementOperators"}));

  private final java.lang.String literalName;

  private ElementOperatorsEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.ElementOperatorsEnum)?other == this:
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

  public static javax.olap.query.enumerations.ElementOperators forName( java.lang.String value ) {
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
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.ElementOperators'");
  }

}
