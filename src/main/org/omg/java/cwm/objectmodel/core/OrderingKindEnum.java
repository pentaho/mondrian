/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public final class OrderingKindEnum
implements org.omg.java.cwm.objectmodel.core.OrderingKind {

  public static final org.omg.java.cwm.objectmodel.core.OrderingKindEnum OK_UNORDERED = new OrderingKindEnum("ok_unordered");

  public static final org.omg.java.cwm.objectmodel.core.OrderingKindEnum OK_ORDERED = new OrderingKindEnum("ok_ordered");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Core","OrderingKind"}));

  private final java.lang.String literalName;

  private OrderingKindEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof org.omg.java.cwm.objectmodel.core.OrderingKindEnum)?other == this:
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

  public static org.omg.java.cwm.objectmodel.core.OrderingKind forName( java.lang.String value ) {
    if ( value.equals(OK_UNORDERED.literalName) ) return OK_UNORDERED;
    if ( value.equals(OK_ORDERED.literalName) ) return OK_ORDERED;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Core.OrderingKind'");
  }

}
