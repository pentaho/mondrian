/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.relationships;



public final class AggregationKindEnum
implements org.omg.java.cwm.objectmodel.relationships.AggregationKind {

  public static final org.omg.java.cwm.objectmodel.relationships.AggregationKindEnum AK_NONE = new AggregationKindEnum("ak_none");

  public static final org.omg.java.cwm.objectmodel.relationships.AggregationKindEnum AK_AGGREGATE = new AggregationKindEnum("ak_aggregate");

  public static final org.omg.java.cwm.objectmodel.relationships.AggregationKindEnum AK_COMPOSITE = new AggregationKindEnum("ak_composite");

  private static final java.util.List typeName = java.util.Collections.unmodifiableList(java.util.Arrays.asList(new String[] {"Relationships","AggregationKind"}));

  private final java.lang.String literalName;

  private AggregationKindEnum( java.lang.String literalName ) {
    this.literalName = literalName;
  }

  public java.util.List refTypeName() {
    return typeName;
  }

  public java.lang.String toString() {
    return literalName;
  }

  public boolean equals( java.lang.Object other ) {
    return (other instanceof org.omg.java.cwm.objectmodel.relationships.AggregationKindEnum)?other == this:
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

  public static org.omg.java.cwm.objectmodel.relationships.AggregationKind forName( java.lang.String value ) {
    if ( value.equals(AK_NONE.literalName) ) return AK_NONE;
    if ( value.equals(AK_AGGREGATE.literalName) ) return AK_AGGREGATE;
    if ( value.equals(AK_COMPOSITE.literalName) ) return AK_COMPOSITE;
    throw new java.lang.IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Relationships.AggregationKind'");
  }

}
