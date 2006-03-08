/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde
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
     */
    public TupleType(Type[] elementTypes) {
        assert elementTypes != null;
        this.elementTypes = (Type[]) elementTypes.clone();
    }

    public boolean usesDimension(Dimension dimension, boolean maybe) {
        for (int i = 0; i < elementTypes.length; i++) {
            Type elementType = elementTypes[i];
            if (elementType.usesDimension(dimension, maybe)) {
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
        for (int i = 0; i < elementTypes.length; i++) {
            Type elementType = elementTypes[i];
            if (elementType instanceof MemberType) {
                MemberType memberType = (MemberType) elementType;
                if (memberType.getDimension().isMeasures()) {
                    return memberType.getValueType();
                }
            }
        }
        return new ScalarType();
    }
}

// End TupleType.java
