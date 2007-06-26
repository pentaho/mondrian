/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.type;

import mondrian.olap.*;

/**
 * Tuple type.
 *
 * @author jhyde
 * @since Feb 17, 2005
 * @version $Id$
 */
public class TupleType implements Type {
    public final Type[] elementTypes;

    /**
     * Creates a type representing a tuple whose fields are the given types.
     *
     * @param elementTypes Array of types of the members in this tuple
     */
    public TupleType(Type[] elementTypes) {
        assert elementTypes != null;
        this.elementTypes = elementTypes.clone();
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
}

// End TupleType.java
