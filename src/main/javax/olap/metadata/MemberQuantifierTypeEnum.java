/*
 * Java(TM) OLAP Interface
 */
package javax.olap.metadata;



public final class MemberQuantifierTypeEnum
implements javax.olap.metadata.MemberQuantifierType {

  public static final javax.olap.metadata.MemberQuantifierTypeEnum ANY = new MemberQuantifierTypeEnum("Any");

  public static final javax.olap.metadata.MemberQuantifierTypeEnum EACH = new MemberQuantifierTypeEnum("Each");

  public static final javax.olap.metadata.MemberQuantifierTypeEnum EVERY = new MemberQuantifierTypeEnum("Every");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Metadata","MemberQuantifierType"}));

  private final java.lang.String literalName;

  private MemberQuantifierTypeEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.metadata.MemberQuantifierTypeEnum)?other == this:
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

  public static javax.olap.metadata.MemberQuantifierType forName( java.lang.String value ) {
    if ( value.equals(ANY.literalName) ) return ANY;
    if ( value.equals(EACH.literalName) ) return EACH;
    if ( value.equals(EVERY.literalName) ) return EVERY;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Metadata.MemberQuantifierType'");
  }

}
