/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class SelectedObjectTypeEnum
implements javax.olap.query.enumerations.SelectedObjectType {

  public static final javax.olap.query.enumerations.SelectedObjectTypeEnum ATTRIBUTE_REFERENCE = new SelectedObjectTypeEnum("AttributeReference");

  public static final javax.olap.query.enumerations.SelectedObjectTypeEnum DERIVED_ATTRIBUTE_REFERENCE = new SelectedObjectTypeEnum("DerivedAttribute");

  public static final javax.olap.query.enumerations.SelectedObjectTypeEnum LITERAL_REFERENCE = new SelectedObjectTypeEnum("LiteralReference");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","SelectedObjectType"}));

  private final java.lang.String literalName;

  private SelectedObjectTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.SelectedObjectTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.SelectedObjectType forName( java.lang.String value ) {
    if ( value.equals(ATTRIBUTE_REFERENCE.literalName) ) return ATTRIBUTE_REFERENCE;
    if ( value.equals(DERIVED_ATTRIBUTE_REFERENCE.literalName) ) return DERIVED_ATTRIBUTE_REFERENCE;
    if ( value.equals(LITERAL_REFERENCE.literalName) ) return LITERAL_REFERENCE;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.SelectedObjectType'");
  }

}
