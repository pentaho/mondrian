/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;
import java.io.*;
import java.util.*;

/**
 * todo:
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public abstract class LevelBase
	extends OlapElementBase
	implements Level
{
	protected HierarchyBase hierarchy;
	protected String name;
	protected String uniqueName;
	protected String description;
	protected int depth;
	protected int levelType;

	// from Element
	public String getQualifiedName() {
		return Util.getRes().getMdxLevelName(getUniqueName()); }

	// from Exp
	public int getType() { return CatLevel; }
	public int getLevelType() { return levelType; }
	public String getUniqueName() { return uniqueName; }
	public String getName() { return name; }
	public String getDescription() { return description; }
	public OlapElement getParent() { return hierarchy; }
	public Hierarchy getHierarchy() { return hierarchy; }
	public Dimension getDimension() { return hierarchy.dimension; }
	public Cube getCube() { return hierarchy.dimension.cube; }
	public int getDepth() { return depth; }

	public Level getChildLevel()
	{
		int childDepth = depth + 1;
		return childDepth < hierarchy.levels.length ?
			hierarchy.levels[childDepth] :
			null;
	}
	public Level getParentLevel()
	{
		int parentDepth = depth - 1;
		return parentDepth >= 0 ?
			hierarchy.levels[parentDepth] :
			null;
	}

	// AdomdLevel and RolapLevel do it differently
	public abstract boolean isAll();

	public boolean isMeasure()
	{
		return hierarchy.getName().equals("Measures");
	}

	public OlapElement lookupChild(NameResolver st, String s)
	{
		return lookupMember(st, s);
	}

	/** find a named member in this level */
	Member lookupMember(NameResolver st, String name)
	{
		if (areMembersUnique()) {
			return hierarchy.lookupRootMember(name);
		} else {
			return null;
		}
	}

	public void accept(Visitor visitor)
	{
		visitor.visit(this);
	}
	public void childrenAccept(Visitor visitor)
	{
		// we don't generally visit child members
	}
}


// End LevelBase.java
