package javax.jmi.model;

import javax.jmi.reflect.*;

public final class VisibilityKindEnum implements VisibilityKind {
    public static final VisibilityKindEnum PUBLIC_VIS = new VisibilityKindEnum("public_vis");
    public static final VisibilityKindEnum PROTECTED_VIS = new VisibilityKindEnum("protected_vis");
    public static final VisibilityKindEnum PRIVATE_VIS = new VisibilityKindEnum("private_vis");

    private static final java.util.List typeName;
    private final String literalName;

    static {
        java.util.ArrayList temp = new java.util.ArrayList();
        temp.add("Model");
        temp.add("VisibilityKind");
        typeName = java.util.Collections.unmodifiableList(temp);
    }

    private VisibilityKindEnum(String literalName) {
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
        if (o instanceof VisibilityKindEnum) return (o == this);
        else if (o instanceof VisibilityKind) return (o.toString().equals(literalName));
        else return ((o instanceof RefEnum) && ((RefEnum) o).refTypeName().equals(typeName) && o.toString().equals(literalName));
    }

    protected Object readResolve() throws java.io.ObjectStreamException {
    	try {
    		return forName(literalName);
    	} catch ( IllegalArgumentException iae ) {
    		throw new java.io.InvalidObjectException(iae.getMessage());
    	}
    }
  public static VisibilityKind forName( java.lang.String value ) {
    if ( value.equals("public_vis") ) return VisibilityKindEnum.PUBLIC_VIS;
    if ( value.equals("protected_vis") ) return VisibilityKindEnum.PROTECTED_VIS;
    if ( value.equals("private_vis") ) return VisibilityKindEnum.PRIVATE_VIS;
    throw new IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Model.VisibilityKind'");
  }
}
