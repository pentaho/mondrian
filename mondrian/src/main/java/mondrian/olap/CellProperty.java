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
