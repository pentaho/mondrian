/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class SortTypeEnum
implements javax.olap.query.enumerations.SortType {

  public static final javax.olap.query.enumerations.SortTypeEnum ASCENDING = new SortTypeEnum("Ascending");

  public static final javax.olap.query.enumerations.SortTypeEnum DESCENDING = new SortTypeEnum("Descending");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","SortType"}));

  private final java.lang.String literalName;

  private SortTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.SortTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.SortType forName( java.lang.String value ) {
    if ( value.equals(ASCENDING.literalName) ) return ASCENDING;
    if ( value.equals(DESCENDING.literalName) ) return DESCENDING;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.SortType'");
  }

}
