/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.Property;
import mondrian.spi.PropertyFormatter;

import org.apache.log4j.Logger;

/**
 * <code>RolapProperty</code> is the definition of a member property.
 *
 * @version $Id$
 * @author jhyde
 */
class RolapProperty extends Property {

    private static final Logger LOGGER = Logger.getLogger(RolapProperty.class);

    private final PropertyFormatter formatter;
    private final String caption;

    final RolapAttribute owningAttribute;
    final RolapAttribute attribute;

    /**
     * Creates a RolapProperty.
     *
     * @param name Name of property
     * @param owningAttribute Attribute that owns this property is based on; or
     *    null if it is an intrinsic property of a level, e.g. level name
     * @param sourceAttribute Attribute that provides the value of this
     *    property; or null if it is
     *    an intrinsic property of a level, e.g. level name
     * @param type Datatype
     * @param formatter Formatter, or null
     * @param caption Caption
     * @param internal Whether property is internal
     */
    RolapProperty(
        String name,
        RolapAttribute owningAttribute,
        RolapAttribute sourceAttribute,
        Datatype type,
        PropertyFormatter formatter,
        String caption,
        boolean internal,
        String description)
    {
        super(name, type, -1, internal, false, false, description);
        this.owningAttribute = owningAttribute;
        this.attribute = sourceAttribute;
        this.caption = caption;
        this.formatter = formatter;
    }

    public PropertyFormatter getFormatter() {
        return formatter;
    }

    /**
     * @return Returns the caption.
     */
    public String getCaption() {
        if (caption == null) {
            return getName();
        }
        return caption;
    }

    /**
     * @return <p>Returns the dependsOnLevelValue setting (if unset,
     * returns false).  This indicates whether the property is
     * functionally dependent on the level with which it is
     * associated.</p>
     *
     * <p>If true, then the property column can be eliminated from
     * the GROUP BY clause for queries on certain databases such
     * as MySQL.</p>
     */
    public boolean dependsOnLevelValue() {
        return true;
    }
}

// End RolapProperty.java
