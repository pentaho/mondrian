/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/
package mondrian.rolap;

import java.lang.reflect.Constructor;

import mondrian.olap.MondrianDef;
import mondrian.olap.Property;
import mondrian.olap.PropertyFormatter;
import mondrian.olap.Util;

import org.apache.log4j.Logger;

/**
 * <code>RolapProperty</code> is the definition of a member property.
 */
class RolapProperty extends Property {

    private static final Logger LOGGER = Logger.getLogger(RolapProperty.class);

    /** Array of RolapProperty of length 0. */
    static final RolapProperty[] emptyArray = new RolapProperty[0];

    private final PropertyFormatter formatter;
    private final String caption;

    /** The column or expression which yields the property's value. */
    private final MondrianDef.Expression exp;


    /**
     * Creates a RolapProperty.
     */
    RolapProperty(
            String name,
            int type,
            MondrianDef.Expression exp,
            String formatterDef,
            String caption) {
        super(name, type, -1, false, false, false, null);
        this.exp = exp;
        this.caption = caption;
        this.formatter = makePropertyFormatter(formatterDef);

    }
    private PropertyFormatter makePropertyFormatter(String formatterDef) {
        if (!Util.isEmpty(formatterDef)) {
            // there is a special property formatter class
            try {
                Class clazz = Class.forName(formatterDef);
                Constructor ctor = clazz.getConstructor(new Class[0]);
                return (PropertyFormatter) ctor.newInstance(new Object[0]);
            } catch (Exception e) {
                StringBuilder buf = new StringBuilder(64);
                buf.append("RolapProperty.makePropertyFormatter: ");
                buf.append("Could not create PropertyFormatter from");
                buf.append("formatterDef \"");
                buf.append(formatterDef);
                buf.append("\"");
                LOGGER.error(buf.toString(), e);
            }
        }
        return null;
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
        if (caption == null)
            return getName();
        return caption;
    }
}

// End RolapProperty.java
