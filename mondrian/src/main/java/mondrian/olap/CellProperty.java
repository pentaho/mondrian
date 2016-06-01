/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1998-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap;

import java.util.List;

/**
 * Represents Cell Property.
 *
 * @author Shishir
 * @since 08 May, 2007
 */

public class CellProperty extends QueryPart {
    private String name;

    public CellProperty(List<Id.Segment> segments) {
        this.name = Util.implode(segments);
    }

    /**
     * checks whether cell property is equals to passed parameter.
     * It adds '[' and ']' before and after the propertyName before comparing.
     * The comparison is case insensitive.
     */
    public boolean isNameEquals(String propertyName) {
        return name.equalsIgnoreCase(Util.quoteMdxIdentifier(propertyName));
    }

    public String toString() {
        return name;
    }
}

// End CellProperty.java
