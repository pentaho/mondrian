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
 * <code>SharedHierarchyUsage</code> todo:
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 **/
class SharedHierarchyUsage extends HierarchyUsage
{
	String sharedHierarchy;

	SharedHierarchyUsage(String factSchema, String factTable, String sharedHierarchy)
	{
		this.factTable = factTable;
		this.factSchema = factSchema;
		this.sharedHierarchy = sharedHierarchy;
	}
	public boolean equals(Object o)
	{
		if (o instanceof SharedHierarchyUsage) {
			SharedHierarchyUsage that = (SharedHierarchyUsage) o;
			return this.factTable.equals(that.factTable) &&
				equals(this.factSchema, that.factSchema) &&
				this.sharedHierarchy.equals(that.sharedHierarchy);
		}
		return false;
	}
	public int hashCode()
	{
		int h = factTable.hashCode(),
			i = factSchema == null ? 1 : factSchema.hashCode(),
			j = sharedHierarchy.hashCode();
		return (h << 8) ^ (i << 4) ^ j;
	}
};

// End SharedHierarchyUsage.java
