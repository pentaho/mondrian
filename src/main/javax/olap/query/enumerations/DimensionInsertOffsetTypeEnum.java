/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class DimensionInsertOffsetTypeEnum
implements javax.olap.query.enumerations.DimensionInsertOffsetType {

  public static final javax.olap.query.enumerations.DimensionInsertOffsetTypeEnum INTEGER_INSERT_OFFSET = new DimensionInsertOffsetTypeEnum("IntegerInsertOffset");

  public static final javax.olap.query.enumerations.DimensionInsertOffsetTypeEnum MEMBER_INSERT_OFFSET = new DimensionInsertOffsetTypeEnum("MemberInsertOffset");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","DimensionInsertOffsetType"}));

  private final java.lang.String literalName;

  private DimensionInsertOffsetTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.DimensionInsertOffsetTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.DimensionInsertOffsetType forName( java.lang.String value ) {
    if ( value.equals(INTEGER_INSERT_OFFSET.literalName) ) return INTEGER_INSERT_OFFSET;
    if ( value.equals(MEMBER_INSERT_OFFSET.literalName) ) return MEMBER_INSERT_OFFSET;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.DimensionInsertOffsetType'");
  }

}
