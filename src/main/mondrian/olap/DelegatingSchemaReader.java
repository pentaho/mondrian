/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2004 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 26, 2003
*/
package mondrian.olap;

import java.util.List;

/**
 * <code>DelegatingSchemaReader</code> implements {@link SchemaReader} by
 * delegating all methods to an underlying {@link SchemaReader}.
 *
 * It is a convenient base class if you want to override just a few of
 * {@link SchemaReader}'s methods.
 *
 * @author jhyde
 * @since Feb 26, 2003
 * @version $Id$
 **/
public class DelegatingSchemaReader implements SchemaReader {
	protected final SchemaReader schemaReader;

	DelegatingSchemaReader(SchemaReader schemaReader) {
		this.schemaReader = schemaReader;
	}

	public Role getRole() {
		return schemaReader.getRole();
	}

	public Member[] getHierarchyRootMembers(Hierarchy hierarchy) {
		return schemaReader.getHierarchyRootMembers(hierarchy);
	}

	public Member getMemberParent(Member member) {
		return schemaReader.getMemberParent(member);
	}

	public Member[] getMemberChildren(Member member) {
		return schemaReader.getMemberChildren(member);
	}

	public Member[] getMemberChildren(Member[] members) {
		return schemaReader.getMemberChildren(members);
	}

    public void getMemberDescendants(Member member, List result, Level level,
            boolean before, boolean self, boolean after) {
        schemaReader.getMemberDescendants(member, result, level,
                before, self, after);
    }

    public int getMemberDepth(Member member) {
		return schemaReader.getMemberDepth(member);
	}

	public Member getMemberByUniqueName(String[] uniqueNameParts,
        boolean failIfNotFound)
    {
		return schemaReader.getMemberByUniqueName(uniqueNameParts,
            failIfNotFound);
	}

    public OlapElement lookupCompound(OlapElement parent, String[] names, 
        boolean failIfNotFound, int category)
    {
        return schemaReader.lookupCompound(parent, names, failIfNotFound,
            category);
    }

    public Member getCalculatedMember(String[] nameParts) {
        return schemaReader.getCalculatedMember(nameParts);
    }

	public void getMemberRange(Level level, Member startMember, Member endMember, List list) {
		schemaReader.getMemberRange(level, startMember, endMember, list);
	}

	public Member getLeadMember(Member member, int n) {
		return schemaReader.getLeadMember(member, n);
	}

	public int compareMembersHierarchically(Member m1, Member m2) {
		return schemaReader.compareMembersHierarchically(m1, m2);
	}

	public OlapElement getElementChild(OlapElement parent, String name) {
		return schemaReader.getElementChild(parent, name);
	}

	public Member[] getLevelMembers(Level level) {
		return schemaReader.getLevelMembers(level);
	}

	public Level[] getHierarchyLevels(Hierarchy hierarchy) {
		return schemaReader.getHierarchyLevels(hierarchy);
	}

	public Member getHierarchyDefaultMember(Hierarchy hierarchy) {
		return schemaReader.getHierarchyDefaultMember(hierarchy);
	}

    public boolean isDrillable(Member member) {
        return schemaReader.isDrillable(member);
    }

    public boolean isVisible(Member member) {
        return schemaReader.isVisible(member);
    }

    public Cube[] getCubes() {
        return schemaReader.getCubes();
    }

	public int getChildrenCountFromCache(Member member) {
		return schemaReader.getChildrenCountFromCache(member);
	}
}

// End DelegatingSchemaReader.java
