package javax.jmi.model;

import javax.jmi.reflect.*;

public final class EvaluationKindEnum implements EvaluationKind {
    public static final EvaluationKindEnum IMMEDIATE = new EvaluationKindEnum("immediate");
    public static final EvaluationKindEnum DEFERRED = new EvaluationKindEnum("deferred");

    private static final java.util.List typeName;
    private final String literalName;

    static {
        java.util.ArrayList temp = new java.util.ArrayList();
        temp.add("Model");
        temp.add("Constraint");
        temp.add("EvaluationKind");
        typeName = java.util.Collections.unmodifiableList(temp);
    }

    private EvaluationKindEnum(String literalName) {
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
        if (o instanceof EvaluationKindEnum) return (o == this);
        else if (o instanceof EvaluationKind) return (o.toString().equals(literalName));
        else return ((o instanceof RefEnum) && ((RefEnum) o).refTypeName().equals(typeName) && o.toString().equals(literalName));
    }

    protected Object readResolve() throws java.io.ObjectStreamException {
    	try {
    		return forName(literalName);
    	} catch ( IllegalArgumentException iae ) {
    		throw new java.io.InvalidObjectException(iae.getMessage());
    	}
    }
  public static EvaluationKind forName( java.lang.String value ) {
    if ( value.equals("immediate") ) return EvaluationKindEnum.IMMEDIATE;
    if ( value.equals("deferred") ) return EvaluationKindEnum.DEFERRED;
    throw new IllegalArgumentException("Unknown enumeration value '"+value+"' for type 'Model.Constraint.EvaluationKind'");
  }
}
