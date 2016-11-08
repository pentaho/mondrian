/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho
// All Rights Reserved.
*/

package mondrian.olap.type;

import mondrian.olap.*;

import java.util.List;

/**
 * Set type.
 *
 * @author jhyde
 * @since Feb 17, 2005
 */
public class SetType implements Type {

    private final Type elementType;
    private final String digest;

    /**
     * Creates a type representing a set of elements of a given type.
     *
     * @param elementType The type of the elements in the set, or null if not
     *   known
     */
    public SetType(Type elementType) {
        if (elementType != null) {
            assert elementType instanceof MemberType
                || elementType instanceof TupleType;
        }
        this.elementType = elementType;
        this.digest = "SetType<" + elementType + ">";
    }

    public int hashCode() {
        return digest.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj instanceof SetType) {
            SetType that = (SetType) obj;
            return Util.equals(this.elementType, that.elementType);
        } else {
            return false;
        }
    }

    public String toString() {
        return digest;
    }

    /**
     * Returns the type of the elements of this set.
     *
     * @return the type of the elements in this set
     */
    public Type getElementType() {
        return elementType;
    }

    public boolean usesDimension(Dimension dimension, boolean definitely) {
        if (elementType == null) {
            return definitely;
        }
        return elementType.usesDimension(dimension, definitely);
    }

    public boolean usesHierarchy(Hierarchy hierarchy, boolean definitely) {
        if (elementType == null) {
            return definitely;
        }
        return elementType.usesHierarchy(hierarchy, definitely);
    }

    public Dimension getDimension() {
        return elementType == null
            ? null
            : elementType.getDimension();
    }

    public Hierarchy getHierarchy() {
        return elementType == null
            ? null
            : elementType.getHierarchy();
    }

    public Level getLevel() {
        return elementType == null
            ? null
            : elementType.getLevel();
    }

    public int getArity() {
        return elementType.getArity();
    }

    public Type computeCommonType(Type type, int[] conversionCount) {
        if (!(type instanceof SetType)) {
            return null;
        }
        SetType that = (SetType) type;
        final Type mostGeneralElementType =
            this.getElementType().computeCommonType(
                that.getElementType(), conversionCount);
        if (mostGeneralElementType == null) {
            return null;
        }
        return new SetType(mostGeneralElementType);
    }

    public boolean isInstance(Object value) {
        if (!(value instanceof List)) {
            return false;
        }
        List list = (List) value;
        for (Object o : list) {
            if (!elementType.isInstance(o)) {
                return false;
            }
        }
        return true;
    }
}

// End SetType.java
