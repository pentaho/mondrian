/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class SetEdgeActionTypeEnum
implements javax.olap.query.enumerations.SetEdgeActionType {

  public static final javax.olap.query.enumerations.SetEdgeActionTypeEnum INITIAL = new SetEdgeActionTypeEnum("Initial");

  public static final javax.olap.query.enumerations.SetEdgeActionTypeEnum APPEND = new SetEdgeActionTypeEnum("Append");

  public static final javax.olap.query.enumerations.SetEdgeActionTypeEnum PREPEND = new SetEdgeActionTypeEnum("Prepend");

  public static final javax.olap.query.enumerations.SetEdgeActionTypeEnum INSERT = new SetEdgeActionTypeEnum("Insert");

  public static final javax.olap.query.enumerations.SetEdgeActionTypeEnum DIFFERENCE = new SetEdgeActionTypeEnum("Difference");

  public static final javax.olap.query.enumerations.SetEdgeActionTypeEnum INTERSECTION = new SetEdgeActionTypeEnum("Intersection");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","SetEdgeActionType"}));

  private final java.lang.String literalName;

  private SetEdgeActionTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.SetEdgeActionTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.SetEdgeActionType forName( java.lang.String value ) {
    if ( value.equals(INITIAL.literalName) ) return INITIAL;
    if ( value.equals(APPEND.literalName) ) return APPEND;
    if ( value.equals(PREPEND.literalName) ) return PREPEND;
    if ( value.equals(INSERT.literalName) ) return INSERT;
    if ( value.equals(DIFFERENCE.literalName) ) return DIFFERENCE;
    if ( value.equals(INTERSECTION.literalName) ) return INTERSECTION;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.SetEdgeActionType'");
  }

}
