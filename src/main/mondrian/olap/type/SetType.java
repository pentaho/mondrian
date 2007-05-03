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

import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;

/**
 * Set type.
 *
 * @author jhyde
 * @since Feb 17, 2005
 * @version $Id$
 */
public class SetType implements Type {

    private final Type elementType;

    /**
     * Creates a type representing a set of elements of a given type.
     *
     * @param elementType The type of the elements in the set, or null if not
     *   known
     */
    public SetType(Type elementType) {
        if (elementType != null) {
            assert elementType instanceof MemberType ||
                    elementType instanceof TupleType;
        }
        this.elementType = elementType;
    }

    /**
     * Returns the type of the elements of this set.
     */
    public Type getElementType() {
        return elementType;
    }

    public boolean usesDimension(Dimension dimension, boolean maybe) {
        if (elementType == null) {
            return maybe;
        }
        return elementType.usesDimension(dimension, maybe);
    }

    public Dimension getDimension() {
        return elementType == null ? null :
                elementType.getDimension();
    }

    public Hierarchy getHierarchy() {
        return elementType == null ? null :
                elementType.getHierarchy();
    }

    public Level getLevel() {
        return elementType == null ? null :
                elementType.getLevel();
    }

    /**
     * Returns the dimensionality of this SetType. If set contains members,
     * returns 1, otherwise returns the width of the tuples.
     *
     * @return Dimensionality of this SetType
     */
    public int getArity() {
        return elementType instanceof TupleType ?
            ((TupleType) elementType).elementTypes.length :
            1;
    }
}

// End SetType.java
