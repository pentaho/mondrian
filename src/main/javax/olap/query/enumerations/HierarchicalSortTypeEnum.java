/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class HierarchicalSortTypeEnum
implements javax.olap.query.enumerations.HierarchicalSortType {

  public static final javax.olap.query.enumerations.HierarchicalSortTypeEnum PARENTS_FIRST = new HierarchicalSortTypeEnum("ParentsFirst");

  public static final javax.olap.query.enumerations.HierarchicalSortTypeEnum CHILDREN_FIRST = new HierarchicalSortTypeEnum("ChildrenFirst");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","HierarchicalSortType"}));

  private final java.lang.String literalName;

  private HierarchicalSortTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.HierarchicalSortTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.HierarchicalSortType forName( java.lang.String value ) {
    if ( value.equals(PARENTS_FIRST.literalName) ) return PARENTS_FIRST;
    if ( value.equals(CHILDREN_FIRST.literalName) ) return CHILDREN_FIRST;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.HierarchicalSortType'");
  }

}
