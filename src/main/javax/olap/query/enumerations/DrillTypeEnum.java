/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class DrillTypeEnum
implements javax.olap.query.enumerations.DrillType {

  public static final javax.olap.query.enumerations.DrillTypeEnum PARENTS = new DrillTypeEnum("Parents");

  public static final javax.olap.query.enumerations.DrillTypeEnum ANCESTORS = new DrillTypeEnum("Ancestors");

  public static final javax.olap.query.enumerations.DrillTypeEnum DESCENDANTS = new DrillTypeEnum("Descendants");

  public static final javax.olap.query.enumerations.DrillTypeEnum SIBLINGS = new DrillTypeEnum("Siblings");

  public static final javax.olap.query.enumerations.DrillTypeEnum CHILDREN = new DrillTypeEnum("Children");

  public static final javax.olap.query.enumerations.DrillTypeEnum ROOTS = new DrillTypeEnum("Roots");

  public static final javax.olap.query.enumerations.DrillTypeEnum LEAVES = new DrillTypeEnum("Leaves");

  public static final javax.olap.query.enumerations.DrillTypeEnum TO_LEVEL = new DrillTypeEnum("ToLevel");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","DrillType"}));

  private final java.lang.String literalName;

  private DrillTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.DrillTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.DrillType forName( java.lang.String value ) {
    if ( value.equals(PARENTS.literalName) ) return PARENTS;
    if ( value.equals(ANCESTORS.literalName) ) return ANCESTORS;
    if ( value.equals(DESCENDANTS.literalName) ) return DESCENDANTS;
    if ( value.equals(SIBLINGS.literalName) ) return SIBLINGS;
    if ( value.equals(CHILDREN.literalName) ) return CHILDREN;
    if ( value.equals(ROOTS.literalName) ) return ROOTS;
    if ( value.equals(LEAVES.literalName) ) return LEAVES;
    if ( value.equals(TO_LEVEL.literalName) ) return TO_LEVEL;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.DrillType'");
  }

}
