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
import java.io.PrintWriter;

/**
 * todo:
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public abstract class HierarchyBase
	extends OlapElementBase
	implements Hierarchy
{
	protected DimensionBase dimension;
	/**
	 * <code>name</code> and <code>subName</code> are the name of the
	 * hierarchy, respectively containing and not containing dimension
	 * name. For example:
	 * <table>
	 * <tr> <th>uniqueName</th>    <th>name</th>        <th>subName</th></tr>
	 * <tr> <td>[Time.Weekly]</td> <td>Time.Weekly</td> <td>Weekly</td></tr>
	 * <tr> <td>[Customers]</td>   <td>Customers</td>   <td>null</td></tr>
	 * </table>
	 **/
	protected String subName;
	protected String name;
	protected String uniqueName;
	protected String description;
	protected LevelBase[] levels;
	protected boolean hasAll;

	// implement MdxElement
	public String getUniqueName() { return uniqueName; }
	public String getName() { return name; } 
	public String getQualifiedName() {
		return Util.getRes().getMdxHierarchyName(getUniqueName());
	}
	public String getDescription() { return description; }
	public Dimension getDimension() { return dimension; }
	public boolean usesDimension(Dimension dimension) {
		return this.dimension == dimension;
	}
	public Level[] getLevels() { return levels; }
	public Hierarchy getHierarchy() { return this; }
	public int getType() { return CatHierarchy; }
	public boolean hasAll() { return hasAll; }

	/** find a child object */
	public OlapElement lookupChild(NameResolver st, String s)
	{
		Level mdxLevel = lookupLevel(s);
		if (mdxLevel != null) {
			return mdxLevel;
		}
		return lookupRootMember(s);
	}

	// implement Hierarchy
	public Member[] getRootMembers()
	{
		return levels[0].getMembers();
	}

	public Member lookupRootMember(String s)
	{
		// Lookup member at first level.
		Member[] rootMembers = getRootMembers();
		for (int i = 0; i < rootMembers.length; i++) {
			if (rootMembers[i].getName().equalsIgnoreCase(s)) {
				return rootMembers[i];
			}
		}
		// If the first level is 'all', lookup member at second level. For
		// example, they could say '[USA]' instead of '[(All
		// Customers)].[USA]'.
		if (hasAll()) {
			return rootMembers[0].lookupChildMember(s);
		}
		return null;
	}

	// implement Hierarchy
	public Level lookupLevel(String s)
	{
		Level[] mdxLevels = getLevels();
		for (int i = 0; i < mdxLevels.length; i++) {
			if (mdxLevels[i].getName().equalsIgnoreCase(s)) {
				return mdxLevels[i];
			}
		}
		return null;
	}

	public Object[] getChildren() { return getLevels(); }

	protected Object[] getAllowedChildren(CubeAccess cubeAccess)
	{
		// cubeAccess sets permissions on hierarchies and members only
		return getChildren();
	}

//  	/** find a named member in this hierarchy */
//  	public abstract Member lookupMember(NameResolver st, String s);

	public void accept(Visitor visitor)
	{
		visitor.visit(this);
	}
	public void childrenAccept(Visitor visitor)
	{
		Level[] levels = getLevels();
		for (int i = 0; i < levels.length; i++) {
			levels[i].accept(visitor);
		}
	}

	public String getAllMemberName()
	{
		boolean alreadyPlural = name.equals("Customers") ||
			name.equals("Promotions") ||
			name.equals("Promotion Media");
		return "All " + name + (alreadyPlural ? "" : "s");
	}
}


// End HierarchyBase.java
