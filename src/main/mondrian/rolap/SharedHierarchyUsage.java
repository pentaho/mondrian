/*
// $Id$
// (C) Copyright 2002 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap;

import mondrian.olap.MondrianDef;

/**
 * A usage of a shared hierarchy in the context of a particular cube.
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 **/
class SharedHierarchyUsage extends HierarchyUsage
{
	String sharedHierarchy;

	SharedHierarchyUsage(MondrianDef.Relation fact, String sharedHierarchy)
	{
		super(fact);
		this.sharedHierarchy = sharedHierarchy;
	}
	public boolean equals(Object o)
	{
		if (!(o instanceof SharedHierarchyUsage)) {
			return false;
		}
		SharedHierarchyUsage that = (SharedHierarchyUsage) o;
		return this.fact.equals(that.fact) &&
			this.sharedHierarchy.equals(that.sharedHierarchy);
	}
	public int hashCode()
	{
		int h = fact.hashCode(),
			j = sharedHierarchy.hashCode();
		return (h << 8) ^ j;
	}
}

// End SharedHierarchyUsage.java
