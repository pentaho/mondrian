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
import mondrian.olap.*;

/**
 * <code>RolapLevel</code> implements {@link Level} for a ROLAP database.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapLevel extends LevelBase
{
	/** For SQL generator. Column which holds key values. */
	String column;
	/** For SQL generator. Column which holds the member ordinal. **/
	String ordinalColumn;
	/** For SQL generator. Whether values of "column" are unique globally
	 * unique (as opposed to unique only within the context of the parent
	 * member). **/
	boolean unique;
	int flags;
	static final int NUMERIC = 1;
	static final int ALL = 2;

	RolapLevel(
		RolapHierarchy hierarchy, int depth, String name, String column,
		String ordinalColumn, int flags)
	{
		this.hierarchy = hierarchy;
		this.name = name;
		this.uniqueName = Util.makeFqName(hierarchy, name);
		this.column = column;
		this.ordinalColumn = ordinalColumn;
		this.flags = flags;
		this.depth = depth;
		this.levelType = Level.STANDARD;
		if (hierarchy.getDimension().getDimensionType() == Dimension.TIME) {
			if (name.equals("Year")) {
				this.levelType = Level.YEARS;
			} else if (name.equals("Quarter")) {
				this.levelType = Level.QUARTERS;
			} else if (name.equals("Month")) {
				this.levelType = Level.MONTHS;
			}
		}
	}

	RolapLevel(RolapHierarchy hierarchy, int depth, MondrianDef.Level xmlLevel)
	{
		this(
			hierarchy, depth, xmlLevel.name, xmlLevel.column,
			xmlLevel.ordinalColumn,
			xmlLevel.type.equals("Numeric") ? NUMERIC : 0);
	}

	void init()
	{
	}

	public Member[] getMembers()
	{
		return ((RolapHierarchy) hierarchy).memberReader.getMembersInLevel(
			this, 0, Integer.MAX_VALUE);
	}
	public Member[] getPeriodsToDate(Member member)
	{
		return ((RolapHierarchy) hierarchy).memberReader.getPeriodsToDate(
			this, (RolapMember) member);
	}
	public boolean isAll()
	{
		return hierarchy.hasAll() && depth == 0;
	}
	public boolean areMembersUnique()
	{
		return depth == 0 ||
			depth == 1 && hierarchy.hasAll();
	}
};



// End RolapLevel.java
