/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class EdgeTypeEnum
implements javax.olap.query.enumerations.EdgeType {

  public static final javax.olap.query.enumerations.EdgeTypeEnum ROW = new EdgeTypeEnum("Row");

  public static final javax.olap.query.enumerations.EdgeTypeEnum COLUMN = new EdgeTypeEnum("Column");

  public static final javax.olap.query.enumerations.EdgeTypeEnum PAGE = new EdgeTypeEnum("Page");

  public static final javax.olap.query.enumerations.EdgeTypeEnum SLICER = new EdgeTypeEnum("Slicer");

  public static final javax.olap.query.enumerations.EdgeTypeEnum OTHER = new EdgeTypeEnum("Other");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","EdgeType"}));

  private final java.lang.String literalName;

  private EdgeTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.EdgeTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.EdgeType forName( java.lang.String value ) {
    if ( value.equals(ROW.literalName) ) return ROW;
    if ( value.equals(COLUMN.literalName) ) return COLUMN;
    if ( value.equals(PAGE.literalName) ) return PAGE;
    if ( value.equals(SLICER.literalName) ) return SLICER;
    if ( value.equals(OTHER.literalName) ) return OTHER;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.EdgeType'");
  }

}
