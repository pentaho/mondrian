/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class DataBasedMemberFilterInputTypeEnum
implements javax.olap.query.enumerations.DataBasedMemberFilterInputType {

  public static final javax.olap.query.enumerations.DataBasedMemberFilterInputTypeEnum ATTRIBUTE_REFERENCE = new DataBasedMemberFilterInputTypeEnum("AttributeReference");

  public static final javax.olap.query.enumerations.DataBasedMemberFilterInputTypeEnum QUALIFIED_MEMBER_REFERENCE = new DataBasedMemberFilterInputTypeEnum("QualifiedMemberReference");

  public static final javax.olap.query.enumerations.DataBasedMemberFilterInputTypeEnum DERIVED_ATTRIBUTE_REFERENCE = new DataBasedMemberFilterInputTypeEnum("DerivedAttributeReference");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","DataBasedMemberFilterInputType"}));

  private final java.lang.String literalName;

  private DataBasedMemberFilterInputTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.DataBasedMemberFilterInputTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.DataBasedMemberFilterInputType forName( java.lang.String value ) {
    if ( value.equals(ATTRIBUTE_REFERENCE.literalName) ) return ATTRIBUTE_REFERENCE;
    if ( value.equals(QUALIFIED_MEMBER_REFERENCE.literalName) ) return QUALIFIED_MEMBER_REFERENCE;
    if ( value.equals(DERIVED_ATTRIBUTE_REFERENCE.literalName) ) return DERIVED_ATTRIBUTE_REFERENCE;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.DataBasedMemberFilterInputType'");
  }

}
