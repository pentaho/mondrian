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
import mondrian.olap.Hierarchy;
import mondrian.olap.Util;

/**
 * A <code>HierarchyUsage</code> is the usage of a hierarchy in the context
 * of a cube. Private hierarchies can only be used in their own
 * cube. Public hierarchies can be used in several cubes. The problem comes
 * when several cubes which the same public hierarchy are brought together
 * in one virtual cube. There are now several usages of the same public
 * hierarchy. Which one to use? It depends upon what measure we are
 * currently using. We should use the hierarchy usage for the fact table
 * which underlies the measure. That is what determines the foreign key to
 * join on.
 *
 * A <code>HierarchyUsage</code> is identified by
 * <code>(hierarchy.sharedHierarchy, factTable)</code> if the hierarchy is
 * shared, or <code>(hierarchy, factTable)</code> if it is private.
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 **/
abstract class HierarchyUsage
{
	/**
	 * Fact table (or relation) which this usage is joining to. This
	 * identifies the usage, and determines which join conditions need to be
	 * used.
	 */
	protected MondrianDef.Relation fact;
	/** The foreign key by which {@link #hierarchy} should be joined to
	 * {@link #factTable}. **/
	String foreignKey;
	/** The primary key column by which {@link hierarchy} should be joined to
	 * {@link #fact}. */
	String primaryKey;
	/** Name of dimension table which contains the primary key for the
	 * hierarchy. (Usually the table of the lowest level of the hierarchy.) */
	MondrianDef.Relation primaryKeyTable;

	HierarchyUsage(MondrianDef.Relation fact) {
		this.fact = fact;
	}
}

// End HierarchyUsage.java
