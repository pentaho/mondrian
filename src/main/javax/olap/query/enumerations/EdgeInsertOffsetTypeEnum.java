/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class EdgeInsertOffsetTypeEnum
implements javax.olap.query.enumerations.EdgeInsertOffsetType {

  public static final javax.olap.query.enumerations.EdgeInsertOffsetTypeEnum INTEGER_INSERT_OFFSET = new EdgeInsertOffsetTypeEnum("IntegerInsertOffset");

  public static final javax.olap.query.enumerations.EdgeInsertOffsetTypeEnum TUPLE_INSERT_OFFSET = new EdgeInsertOffsetTypeEnum("TupleInsertOffset");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","EdgeInsertOffsetType"}));

  private final java.lang.String literalName;

  private EdgeInsertOffsetTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.EdgeInsertOffsetTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.EdgeInsertOffsetType forName( java.lang.String value ) {
    if ( value.equals(INTEGER_INSERT_OFFSET.literalName) ) return INTEGER_INSERT_OFFSET;
    if ( value.equals(TUPLE_INSERT_OFFSET.literalName) ) return TUPLE_INSERT_OFFSET;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.EdgeInsertOffsetType'");
  }

}
