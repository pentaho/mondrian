/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class RankingTypeEnum
implements javax.olap.query.enumerations.RankingType {

  public static final javax.olap.query.enumerations.RankingTypeEnum TOP = new RankingTypeEnum("Top");

  public static final javax.olap.query.enumerations.RankingTypeEnum BOTTOM = new RankingTypeEnum("Bottom");

  public static final javax.olap.query.enumerations.RankingTypeEnum TOP_BOTTOM = new RankingTypeEnum("TopBottom");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","RankingType"}));

  private final java.lang.String literalName;

  private RankingTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.RankingTypeEnum)?other == this:
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

  public static javax.olap.query.enumerations.RankingType forName( java.lang.String value ) {
    if ( value.equals(TOP.literalName) ) return TOP;
    if ( value.equals(BOTTOM.literalName) ) return BOTTOM;
    if ( value.equals(TOP_BOTTOM.literalName) ) return TOP_BOTTOM;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.RankingType'");
  }

}
