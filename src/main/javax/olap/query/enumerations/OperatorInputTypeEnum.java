/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class OperatorInputTypeEnum
implements javax.olap.query.enumerations.OperatorInputType {

  public static final javax.olap.query.enumerations.OperatorInputTypeEnum OPERATOR_INPUT_TYPE = new OperatorInputTypeEnum("OperatorInputType");

  public static final javax.olap.query.enumerations.OperatorInputTypeEnum ATTRIBUTE_REFERENCE = new OperatorInputTypeEnum("AttributeReference");

  public static final javax.olap.query.enumerations.OperatorInputTypeEnum MEMBER_REFERENCE = new OperatorInputTypeEnum("MemberReference");

  public static final javax.olap.query.enumerations.OperatorInputTypeEnum QUALIFIED_MEMBER_REFERENCE = new OperatorInputTypeEnum("QualifiedMemberReference");

  public static final javax.olap.query.enumerations.OperatorInputTypeEnum DERIVED_ATTRIBUTE_REFERENCE = new OperatorInputTypeEnum("DerivedAttributeReference");

  public static final javax.olap.query.enumerations.OperatorInputTypeEnum LITERAL_REFERENCE = new OperatorInputTypeEnum("LiteralReference");

  public static final javax.olap.query.enumerations.OperatorInputTypeEnum OPERATOR_REFERENCE = new OperatorInputTypeEnum("OperatorReference");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","OperatorInputType"}));

  private final java.lang.String literalName;

  private OperatorInputTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.OperatorInputTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.OperatorInputType forName( java.lang.String value ) {
    if ( value.equals(OPERATOR_INPUT_TYPE.literalName) ) return OPERATOR_INPUT_TYPE;
    if ( value.equals(ATTRIBUTE_REFERENCE.literalName) ) return ATTRIBUTE_REFERENCE;
    if ( value.equals(MEMBER_REFERENCE.literalName) ) return MEMBER_REFERENCE;
    if ( value.equals(QUALIFIED_MEMBER_REFERENCE.literalName) ) return QUALIFIED_MEMBER_REFERENCE;
    if ( value.equals(DERIVED_ATTRIBUTE_REFERENCE.literalName) ) return DERIVED_ATTRIBUTE_REFERENCE;
    if ( value.equals(LITERAL_REFERENCE.literalName) ) return LITERAL_REFERENCE;
    if ( value.equals(OPERATOR_REFERENCE.literalName) ) return OPERATOR_REFERENCE;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.OperatorInputType'");
  }

}
