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

/**
 * todo:
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public abstract class DimensionBase
	extends OlapElementBase
	implements Dimension
{
	protected CubeBase cube;
	protected String name;
	protected String uniqueName;
	protected String description;
	protected HierarchyBase[] hierarchies;
	protected int ordinal;
	protected int dimensionType;

	// implement Element
	public OlapElement getParent() { return cube; }
	public String getUniqueName() { return uniqueName; }
	public String getName() { return name; }
	public String getDescription() { return description; }
	public Hierarchy[] getHierarchies() { return hierarchies; }
	public Cube getCube() { return cube; }
	public int getType() { return CatDimension; }
	public int getDimensionType() { return dimensionType; } 
	public String getQualifiedName() {
		return Util.getRes().getMdxDimensionName(getUniqueName());
	}
	public boolean isMeasures()	{
		return getUniqueName().equals(CONST_MEASURES);
	}
	public int getOrdinal() { return ordinal; } 
	public OlapElement lookupChild(NameResolver st, String s)
	{
		Hierarchy mdxHierarchy = lookupHierarchy(st, s);
		
		// If the user is looking for [Marital Status].[Marital Status] we
		// should not return mdxHierarchy "Marital Status", because he is
		// looking for level - we can check that by checking of hierarchy and
		// dimension name is the same.
		if (mdxHierarchy != null &&
			!mdxHierarchy.getName().equalsIgnoreCase(getName())) {
			return mdxHierarchy;
		}

		// Defer to the default hierarchy.
		return getHierarchy().lookupChild(st, s);
	}

	public Hierarchy lookupHierarchy(NameResolver st, String s)
	{
		Hierarchy[] mdxHierarchies = getHierarchies();
		for (int i = 0; i < mdxHierarchies.length; i++) {
			if (mdxHierarchies[i].getName().equalsIgnoreCase(s))
				return mdxHierarchies[i];
		}
		return null;
	}

//  	public Level lookupLevel(NameResolver st, String s)
//  	{
//  		Hierarchy[] mdxHierarchies = getHierarchies();
//  		for (int i = 0; i < mdxHierarchies.length; i++) {
//  			Level mdxLevel = mdxHierarchies[i].lookupLevel(st, s);
//  			if (mdxLevel != null)
//  				return mdxLevel;
//  		}
//  		return null;
//  	}

//  	public Member lookupMember(NameResolver st, String s)
//  	{
//  		Hierarchy[] mdxHierarchies = getHierarchies();
//  		for (int i = 0; i < mdxHierarchies.length; i++) {
//  			Member mdxMember = mdxHierarchies[i].lookupMember(st, s);
//  			if (mdxMember != null)
//  				return mdxMember;
//  		}
//  		return null;
//  	}

	public Object[] getChildren() {return getHierarchies();}
	
	protected Object[] getAllowedChildren(CubeAccess cubeAccess)
	{
		java.util.Vector vMdxHierarchies = new java.util.Vector();
		Hierarchy[] mdxHierarchies = getHierarchies();
		for (int i = 0; i < mdxHierarchies.length; i++) {
			if (cubeAccess.isHierarchyAllowed(mdxHierarchies[i]))
				vMdxHierarchies.addElement(mdxHierarchies[i]);
		}
		mdxHierarchies = new Hierarchy[vMdxHierarchies.size()];
		vMdxHierarchies.copyInto(mdxHierarchies);
		return mdxHierarchies;
	}

	public void accept(Visitor visitor)
	{
		visitor.visit(this);
	}
	public void childrenAccept(Visitor visitor)
	{
		Hierarchy[] hierarchies = getHierarchies();
		for (int i = 0; i < hierarchies.length; i++) {
			hierarchies[i].accept(visitor);
		}
	}
}


// End DimensionBase.java
