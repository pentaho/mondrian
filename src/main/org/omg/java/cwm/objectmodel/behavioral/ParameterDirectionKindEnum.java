/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.behavioral;



public final class ParameterDirectionKindEnum
implements org.omg.java.cwm.objectmodel.behavioral.ParameterDirectionKind {

  public static final org.omg.java.cwm.objectmodel.behavioral.ParameterDirectionKindEnum PDK_IN = new ParameterDirectionKindEnum("pdk_in");

  public static final org.omg.java.cwm.objectmodel.behavioral.ParameterDirectionKindEnum PDK_INOUT = new ParameterDirectionKindEnum("pdk_inout");

  public static final org.omg.java.cwm.objectmodel.behavioral.ParameterDirectionKindEnum PDK_OUT = new ParameterDirectionKindEnum("pdk_out");

  public static final org.omg.java.cwm.objectmodel.behavioral.ParameterDirectionKindEnum PDK_RETURN = new ParameterDirectionKindEnum("pdk_return");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Behavioral","ParameterDirectionKind"}));

  private final java.lang.String literalName;

  private ParameterDirectionKindEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof org.omg.java.cwm.objectmodel.behavioral.ParameterDirectionKindEnum)?other == this:
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

  public static org.omg.java.cwm.objectmodel.behavioral.ParameterDirectionKind forName( java.lang.String value ) {
    if ( value.equals(PDK_IN.literalName) ) return PDK_IN;
    if ( value.equals(PDK_INOUT.literalName) ) return PDK_INOUT;
    if ( value.equals(PDK_OUT.literalName) ) return PDK_OUT;
    if ( value.equals(PDK_RETURN.literalName) ) return PDK_RETURN;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Behavioral.ParameterDirectionKind'");
  }

}
