/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class HierarchyFilterTypeEnum
implements javax.olap.query.enumerations.HierarchyFilterType {

  public static final javax.olap.query.enumerations.HierarchyFilterTypeEnum ALL_MEMBERS = new HierarchyFilterTypeEnum("AllMembers");

  public static final javax.olap.query.enumerations.HierarchyFilterTypeEnum BOTTOM_LEVEL = new HierarchyFilterTypeEnum("BottomLevel");

  public static final javax.olap.query.enumerations.HierarchyFilterTypeEnum TOP_LEVEL = new HierarchyFilterTypeEnum("TopLevel");

  public static final javax.olap.query.enumerations.HierarchyFilterTypeEnum LEAF_MEMBERS = new HierarchyFilterTypeEnum("LeafMembers");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","HierarchyFilterType"}));

  private final java.lang.String literalName;

  private HierarchyFilterTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.HierarchyFilterTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.HierarchyFilterType forName( java.lang.String value ) {
    if ( value.equals(ALL_MEMBERS.literalName) ) return ALL_MEMBERS;
    if ( value.equals(BOTTOM_LEVEL.literalName) ) return BOTTOM_LEVEL;
    if ( value.equals(TOP_LEVEL.literalName) ) return TOP_LEVEL;
    if ( value.equals(LEAF_MEMBERS.literalName) ) return LEAF_MEMBERS;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.HierarchyFilterType'");
  }

}
