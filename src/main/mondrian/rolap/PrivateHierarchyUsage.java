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

/**
 * <code>PrivateHierarchyUsage</code> todo:
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 **/
class PrivateHierarchyUsage extends HierarchyUsage
{
	RolapHierarchy hierarchy;

	PrivateHierarchyUsage(
		String factSchema, String factTable, RolapHierarchy hierarchy)
	{
		this.factTable = factTable;
		this.factSchema = factSchema;
		this.hierarchy = hierarchy;
	}
	public boolean equals(Object o)
	{
		if (o instanceof PrivateHierarchyUsage) {
			PrivateHierarchyUsage that = (PrivateHierarchyUsage) o;
			return this.factTable.equals(that.factTable) &&
				equals(this.factSchema, that.factSchema) &&
				this.hierarchy.equals(that.hierarchy);
		}
		return false;
	}
	public int hashCode()
	{
		int h = factTable.hashCode(),
			i = factSchema == null ? 1 : factSchema.hashCode(),
			j = hierarchy.hashCode();
		return (h << 8) ^ (i << 4) ^ j;
	}
}


// End PrivateHierarchyUsage.java
