/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.enumerations;



public final class OperatorEnum
implements javax.olap.query.enumerations.Operator {

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Enumerations","Operator"}));

  private final java.lang.String literalName;

  private OperatorEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof javax.olap.query.enumerations.OperatorEnum)?other == this:
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

  public static javax.olap.query.enumerations.Operator forName( java.lang.String value ) {
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Enumerations.Operator'");
  }

}
