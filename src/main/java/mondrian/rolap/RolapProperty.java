/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.spi.PropertyFormatter;

import org.apache.log4j.Logger;

import java.util.Map;

/**
 * <code>RolapProperty</code> is the definition of a member property.
 *
 * @author jhyde
 */
public class RolapProperty extends Property implements Annotated {

    private static final Logger LOGGER = Logger.getLogger(RolapProperty.class);

    private final PropertyFormatter formatter;
    private final Larder larder;

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
     * @param internal Whether property is internal
     */
    RolapProperty(
        String name,
        RolapAttribute owningAttribute,
        RolapAttribute sourceAttribute,
        Datatype type,
        PropertyFormatter formatter,
        boolean internal,
        Larder larder)
    {
        super(
            name, type, -1, internal, false, false,
            Larders.getDescription(larder));
        this.owningAttribute = owningAttribute;
        this.attribute = sourceAttribute;
        this.formatter = formatter;
        this.larder = larder;
    }

    public PropertyFormatter getFormatter() {
        return formatter;
    }

    public Map<String, Annotation> getAnnotationMap() {
        return larder.getAnnotationMap();
    }

    public RolapAttribute getAttribute() {
        return attribute;
    }

    /**
     * @return Returns the caption.
     */
    public String getCaption() {
        return Larders.getCaption(this, larder);
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
