package javax.jmi.model;

import javax.jmi.reflect.*;

public final class DirectionKindEnum implements DirectionKind {
    public static final DirectionKindEnum IN_DIR = new DirectionKindEnum("in_dir");
    public static final DirectionKindEnum OUT_DIR = new DirectionKindEnum("out_dir");
    public static final DirectionKindEnum INOUT_DIR = new DirectionKindEnum("inout_dir");
    public static final DirectionKindEnum RETURN_DIR = new DirectionKindEnum("return_dir");

    private static final java.util.List typeName;
    private final String literalName;

    static {
        java.util.ArrayList temp = new java.util.ArrayList();
        temp.add("Model");
        temp.add("DirectionKind");
        typeName = java.util.Collections.unmodifiableList(temp);
    }

    private DirectionKindEnum(String literalName) {
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
        if (o instanceof DirectionKindEnum) return (o == this);
        else if (o instanceof DirectionKind) return (o.toString().equals(literalName));
        else return ((o instanceof RefEnum) && ((RefEnum) o).refTypeName().equals(typeName) && o.toString().equals(literalName));
    }

    protected Object readResolve() throws java.io.ObjectStreamException {
    	try {
    		return forName(literalName);
    	} catch ( IllegalArgumentException iae ) {
    		throw new java.io.InvalidObjectException(iae.getMessage());
    	}
    }
  public static DirectionKind forName( java.lang.String value ) {
    if ( value.equals("in_dir") ) return DirectionKindEnum.IN_DIR;
    if ( value.equals("out_dir") ) return DirectionKindEnum.OUT_DIR;
    if ( value.equals("inout_dir") ) return DirectionKindEnum.INOUT_DIR;
    if ( value.equals("return_dir") ) return DirectionKindEnum.RETURN_DIR;
    throw new IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Model.DirectionKind'");
  }
}
