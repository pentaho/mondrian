/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.type;

import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;

/**
 * Base class for types which represent scalar values.
 *
 * <p>An instance of this class means a scalar value of unknown type.
 * Usually one of the derived classes {@link NumericType},
 * {@link StringType}, {@link BooleanType} is used instead.
 *
 * @author jhyde
 * @since Feb 17, 2005
 * @version $Id$
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
            && type instanceof ScalarType) {
            return type;
        } else if (this.getClass() == ScalarType.class
            && type instanceof ScalarType) {
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
}

// End ScalarType.java
