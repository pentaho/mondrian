/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.MondrianDef;
import mondrian.olap.Property;
import mondrian.spi.PropertyFormatter;

import org.apache.log4j.Logger;

/**
 * <code>RolapProperty</code> is the definition of a member property.
 *
 * @author jhyde
 */
class RolapProperty extends Property {

    private static final Logger LOGGER = Logger.getLogger(RolapProperty.class);

    /** Array of RolapProperty of length 0. */
    static final RolapProperty[] emptyArray = new RolapProperty[0];

    private final PropertyFormatter formatter;
    private final String caption;
    private final boolean dependsOnLevelValue;

    /** The column or expression which yields the property's value. */
    private final MondrianDef.Expression exp;


    /**
     * Creates a RolapProperty.
     *
     * @param name Name of property
     * @param type Datatype
     * @param exp Expression for property's value; often a literal
     * @param formatter A property formatter, or null
     * @param caption Caption
     * @param dependsOnLevelValue Whether the property is functionally dependent
     *     on the level with which it is associated
     * @param internal Whether property is internal
     */
    RolapProperty(
        String name,
        Datatype type,
        MondrianDef.Expression exp,
        PropertyFormatter formatter,
        String caption,
        Boolean dependsOnLevelValue,
        boolean internal,
        String description)
    {
        super(name, type, -1, internal, false, false, description);
        this.exp = exp;
        this.caption = caption;
        this.formatter = formatter;
        this.dependsOnLevelValue =
            dependsOnLevelValue != null && dependsOnLevelValue;
    }

    MondrianDef.Expression getExp() {
        return exp;
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
        return dependsOnLevelValue;
    }
}

// End RolapProperty.java
