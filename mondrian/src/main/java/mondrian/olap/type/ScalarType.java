/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.olap.type;

import mondrian.olap.*;

/**
 * Base class for types which represent scalar values.
 *
 * <p>An instance of this class means a scalar value of unknown type.
 * Usually one of the derived classes {@link NumericType},
 * {@link StringType}, {@link BooleanType} is used instead.
 *
 * @author jhyde
 * @since Feb 17, 2005
 */
public class ScalarType implements Type {
    private final String digest;

    /**
     * Creates a ScalarType.
     */
    public ScalarType() {
        this("SCALAR");
    }

    /**
     * Creates a ScalarType (or subtype) with a given digest.
     *
     * <p>The digest is used for {@link #toString()} and {@link #hashCode()}.
     *
     * @param digest Description of this type
     */
    protected ScalarType(String digest) {
        this.digest = digest;
    }

    public int hashCode() {
        return digest.hashCode();
    }

    public boolean equals(Object obj) {
        return obj != null
            && obj.getClass() == ScalarType.class;
    }

    public String toString() {
        return digest;
    }

    public boolean usesDimension(Dimension dimension, boolean definitely) {
        return false;
    }

    public boolean usesHierarchy(Hierarchy hierarchy, boolean definitely) {
        return false;
    }

    public Hierarchy getHierarchy() {
        return null;
    }

    public Level getLevel() {
        return null;
    }

    public Type computeCommonType(Type type, int[] conversionCount) {
        if (this.equals(type)) {
            return this;
        } else if (type instanceof NullType) {
            return this;
        } else if (this instanceof NullType
            && type instanceof ScalarType)
        {
            return type;
        } else if (this.getClass() == ScalarType.class
            && type instanceof ScalarType)
        {
            return this;
        } else if (type.getClass() == ScalarType.class) {
            return type;
        } else if (type instanceof ScalarType) {
            return new ScalarType();
        } else if (type instanceof MemberType) {
            return computeCommonType(
                ((MemberType) type).getValueType(),
                conversionCount);
        } else if (type instanceof TupleType) {
            return computeCommonType(
                ((TupleType) type).getValueType(),
                conversionCount);
        } else {
            return null;
        }
    }

    public Dimension getDimension() {
        return null;
    }

    public boolean isInstance(Object value) {
        // Somewhat pessimistic.
        return false;
    }

    public int getArity() {
        return 1;
    }
}

// End ScalarType.java
