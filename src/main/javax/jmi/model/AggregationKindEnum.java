package javax.jmi.model;

import javax.jmi.reflect.*;

public final class AggregationKindEnum implements AggregationKind {
    public static final AggregationKindEnum NONE = new AggregationKindEnum("none");
    public static final AggregationKindEnum SHARED = new AggregationKindEnum("shared");
    public static final AggregationKindEnum COMPOSITE = new AggregationKindEnum("composite");

    private static final java.util.List typeName;
    private final String literalName;

    static {
        java.util.ArrayList temp = new java.util.ArrayList();
        temp.add("Model");
        temp.add("AggregationKind");
        typeName = java.util.Collections.unmodifiableList(temp);
    }

    private AggregationKindEnum(String literalName) {
        this.literalName = literalName;
    }

    public java.util.List refTypeName() {
        return typeName;
    }

    public String toString() {
        return literalName;
    }

    public int hashCode() {
        return literalName.hashCode();
    }

    public boolean equals(Object o) {
        if (o instanceof AggregationKindEnum) return (o == this);
        else if (o instanceof AggregationKind) return (o.toString().equals(literalName));
        else return ((o instanceof RefEnum) && ((RefEnum) o).refTypeName().equals(typeName) && o.toString().equals(literalName));
    }

    protected Object readResolve() throws java.io.ObjectStreamException {
    	try {
    		return forName(literalName);
    	} catch ( IllegalArgumentException iae ) {
    		throw new java.io.InvalidObjectException(iae.getMessage());
    	}
    }
  public static AggregationKind forName( java.lang.String value ) {
    if ( value.equals("none") ) return AggregationKindEnum.NONE;
    if ( value.equals("shared") ) return AggregationKindEnum.SHARED;
    if ( value.equals("composite") ) return AggregationKindEnum.COMPOSITE;
    throw new IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Model.AggregationKind'");
  }
}
