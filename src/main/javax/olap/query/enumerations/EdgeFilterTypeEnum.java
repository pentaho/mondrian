/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class EdgeFilterTypeEnum
implements javax.olap.query.enumerations.EdgeFilterType {

  public static final javax.olap.query.enumerations.EdgeFilterTypeEnum TUPLE_FILTER = new EdgeFilterTypeEnum("TupleFilter");

  public static final javax.olap.query.enumerations.EdgeFilterTypeEnum EDGE_DRILL_FILTER = new EdgeFilterTypeEnum("EdgeDrillFilter");

  public static final javax.olap.query.enumerations.EdgeFilterTypeEnum SUPPRESS_EDGE_MEMBER_FILTER = new EdgeFilterTypeEnum("SuppressEdgeMemberFilter");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","EdgeFilterType"}));

  private final java.lang.String literalName;

  private EdgeFilterTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.EdgeFilterTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.EdgeFilterType forName( java.lang.String value ) {
    if ( value.equals(TUPLE_FILTER.literalName) ) return TUPLE_FILTER;
    if ( value.equals(EDGE_DRILL_FILTER.literalName) ) return EDGE_DRILL_FILTER;
    if ( value.equals(SUPPRESS_EDGE_MEMBER_FILTER.literalName) ) return SUPPRESS_EDGE_MEMBER_FILTER;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.EdgeFilterType'");
  }

}
