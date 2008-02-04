/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.type;

import mondrian.olap.*;

import java.util.Arrays;

/**
 * Tuple type.
 *
 * @author jhyde
 * @since Feb 17, 2005
 * @version $Id$
 */
public class TupleType implements Type {
    public final Type[] elementTypes;
    private final String digest;

    /**
     * Creates a type representing a tuple whose fields are the given types.
     *
     * @param elementTypes Array of types of the members in this tuple
     */
    public TupleType(Type[] elementTypes) {
        assert elementTypes != null;
        this.elementTypes = elementTypes.clone();

        final StringBuilder buf = new StringBuilder();
        buf.append("TupleType<");
        int k = 0;
        for (Type elementType : elementTypes) {
            if (k++ > 0) {
                buf.append(", ");
            }
            buf.append(elementType);
        }
        buf.append(">");
        digest = buf.toString();
    }

    public String toString() {
        return digest;
    }

    public boolean equals(Object obj) {
        if (obj instanceof TupleType) {
            TupleType that = (TupleType) obj;
            return Arrays.equals(this.elementTypes, that.elementTypes);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return digest.hashCode();
    }

    public boolean usesDimension(Dimension dimension, boolean definitely) {
        for (Type elementType : elementTypes) {
            if (elementType.usesDimension(dimension, definitely)) {
                return true;
            }
        }
        return false;
    }

    public Dimension getDimension() {
        throw new UnsupportedOperationException();
    }

    public Hierarchy getHierarchy() {
        throw new UnsupportedOperationException();
    }

    public Level getLevel() {
        throw new UnsupportedOperationException();
    }

    public Type getValueType() {
        for (Type elementType : elementTypes) {
            if (elementType instanceof MemberType) {
                MemberType memberType = (MemberType) elementType;
                Dimension dimension = memberType.getDimension();
                if (dimension != null && dimension.isMeasures()) {
                    return memberType.getValueType();
                }
            }
        }
        return new ScalarType();
    }

    public Type computeCommonType(Type type, int[] conversionCount) {
        if (type instanceof ScalarType) {
            return getValueType().computeCommonType(type, conversionCount);
        }
        if (type instanceof MemberType) {
            return getValueType().computeCommonType(type, conversionCount);
        }
        if (!(type instanceof TupleType)) {
            return null;
        }
        TupleType that = (TupleType) type;
        if (this.elementTypes.length !=
            that.elementTypes.length) {
            return null;
        }
        final Type[] elementTypes =
            new Type[this.elementTypes.length];
        for (int i = 0; i < elementTypes.length; i++) {
            elementTypes[i] =
                this.elementTypes[i].computeCommonType(
                    that.elementTypes[i], conversionCount);
            if (elementTypes[i] == null) {
                return null;
            }
        }
        return new TupleType(elementTypes);
    }
}

// End TupleType.java
