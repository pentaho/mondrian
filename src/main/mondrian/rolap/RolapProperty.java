/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
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

/**
 * <code>RolapProperty</code> is the definition of a member property.
 */
class RolapProperty extends Property {
    /** Array of RolapProperty of length 0. */
    static final RolapProperty[] emptyArray = new RolapProperty[0];

    private PropertyFormatter formatter = null;
    private String caption=  null;

    RolapProperty(String name, int type, MondrianDef.Expression exp, String formatterDef, String caption) {
        super(name, type);
        this.exp = exp;
        this.caption = caption;
        if (!Util.isEmpty(formatterDef)) {
            // there is a special property formatter class
            try {
                Class clazz = Class.forName(formatterDef);
                Constructor ctor = clazz.getConstructor(new Class[0]);
                formatter = (PropertyFormatter) ctor.newInstance(new Object[0]);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public PropertyFormatter getFormatter() {
        return formatter;
    }

    /** The column or expression which yields the property's value. */
    MondrianDef.Expression exp;

    /**
     * @return Returns the caption.
     */
    public String getCaption() {
        return caption;
    }
}

// End RolapProperty.java
