package javax.jmi.model;

import javax.jmi.reflect.*;

public final class ScopeKindEnum implements ScopeKind {
    public static final ScopeKindEnum INSTANCE_LEVEL = new ScopeKindEnum("instance_level");
    public static final ScopeKindEnum CLASSIFIER_LEVEL = new ScopeKindEnum("classifier_level");

    private static final java.util.List typeName;
    private final String literalName;

    static {
        java.util.ArrayList temp = new java.util.ArrayList();
        temp.add("Model");
        temp.add("ScopeKind");
        typeName = java.util.Collections.unmodifiableList(temp);
    }

    private ScopeKindEnum(String literalName) {
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
        if (o instanceof ScopeKindEnum) return (o == this);
        else if (o instanceof ScopeKind) return (o.toString().equals(literalName));
        else return ((o instanceof RefEnum) && ((RefEnum) o).refTypeName().equals(typeName) && o.toString().equals(literalName));
    }

    protected Object readResolve() throws java.io.ObjectStreamException {
    	try {
    		return forName(literalName);
    	} catch ( IllegalArgumentException iae ) {
    		throw new java.io.InvalidObjectException(iae.getMessage());
    	}
    }
  public static ScopeKind forName( java.lang.String value ) {
    if ( value.equals("instance_level") ) return ScopeKindEnum.INSTANCE_LEVEL;
    if ( value.equals("classifier_level") ) return ScopeKindEnum.CLASSIFIER_LEVEL;
    throw new IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Model.ScopeKind'");
  }
}
