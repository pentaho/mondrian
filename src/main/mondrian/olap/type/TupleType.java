/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.type;

import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Util;

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

    public boolean usesDimension(Dimension dimension) {
        for (int i = 0; i < elementTypes.length; i++) {
            Type elementType = elementTypes[i];
            if (elementType.usesDimension(dimension)) {
                return true;
            }
        }
        return false;
    }

    public Hierarchy getHierarchy() {
        throw new UnsupportedOperationException();
    }

    public Level getLevel() {
        throw new UnsupportedOperationException();
    }
}

// End SetType.java
