/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/
package mondrian.rolap;

import mondrian.olap.MondrianDef;
import mondrian.olap.Property;

/**
 * <code>RolapProperty</code> is the definition of a member property.
 */
class RolapProperty extends Property {
    /** Array of RolapProperty of length 0. */
    static final RolapProperty[] emptyArray = new RolapProperty[0];

    RolapProperty(String name, int type, MondrianDef.Expression exp) {
		super(name, type);
		this.exp = exp;
	}
	/** The column or expression which yields the property's value. */
	MondrianDef.Expression exp;
}

// End RolapProperty.java
