/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap;

import mondrian.olap.MondrianDef;

/**
 * <code>PrivateHierarchyUsage</code> represents the usage of a private
 * hierarchy by a cube. (Since the hierarchy is private, there can be only one
 * such usage.)
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 **/
class PrivateHierarchyUsage extends HierarchyUsage
{
	RolapHierarchy hierarchy;

	PrivateHierarchyUsage(MondrianDef.Relation fact, RolapHierarchy hierarchy)
    {
		super(fact, hierarchy.foreignKey);
		this.hierarchy = hierarchy;
	}

	public boolean equals(Object o)
	{
		if (!(o instanceof PrivateHierarchyUsage)) {
			return false;
		}
		PrivateHierarchyUsage that = (PrivateHierarchyUsage) o;
		return this.fact.equals(that.fact) &&
				this.hierarchy.equals(that.hierarchy);
	}

	public int hashCode()
	{
		int h = fact.hashCode(),
			i = hierarchy.hashCode();
		return (h << 8) ^ i;
	}
}

// End PrivateHierarchyUsage.java
