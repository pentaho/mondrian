/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 22 December, 2001
*/

package mondrian.rolap;
import mondrian.olap.Util;
import mondrian.olap.MondrianDef;
import mondrian.rolap.sql.SqlQuery;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * <code>ArrayMemberSource</code> implements a flat, static hierarchy. There is
 * no root member, and all members are siblings.
 *
 * @author jhyde
 * @since 22 December, 2001
 * @version $Id$
 **/
abstract class ArrayMemberSource implements MemberSource
{
	RolapHierarchy hierarchy;
	RolapMember[] members;
	ArrayMemberSource(RolapHierarchy hierarchy, RolapMember[] members)
	{
		this.hierarchy = hierarchy;
		this.members = members;
	}
	// implement MemberReader
	public RolapHierarchy getHierarchy()
	{
		return hierarchy;
	}

	// implement MemberSource
	public void setCache(MemberCache cache)
	{
		// we don't care about a cache -- we would not use it
	}
	// implement MemberReader
	public RolapMember[] getMembers()
	{
		return members;
	}
	// implement MemberReader
	public int getMemberCount()
	{
		return members.length;
	}
	public RolapMember[] getRootMembers()
	{
		return new RolapMember[0];
	}
	public RolapMember[] getMemberChildren(RolapMember[] parentOlapMembers)
	{
		return new RolapMember[0];
	}

	public RolapMember lookupMember(String uniqueName, boolean failIfNotFound) {
		for (int i = 0; i < members.length; i++) {
			RolapMember member = members[i];
			if (member.getUniqueName().equals(uniqueName)) {
				return member;
			}
		}
		if (failIfNotFound) {
			throw Util.getRes().newMdxCantFindMember(uniqueName);
		} else {
			return null;
		}
	}
}

class HasBoughtDairySource extends ArrayMemberSource
{
	private RolapHierarchy hierarchy;

	public HasBoughtDairySource(
		RolapHierarchy hierarchy, Properties properties)
	{
		super(hierarchy, new Thunk(hierarchy).getMembers());
		this.hierarchy = hierarchy;
	}

	/**
	 * Because Java won't allow us to call methods before constructing {@link
	 * HasBoughtDairyReader}'s base class.
	 **/
	private static class Thunk
	{
		RolapHierarchy hierarchy;

		Thunk(RolapHierarchy hierarchy)
		{
			this.hierarchy = hierarchy;
		}
		RolapMember[] getMembers()
		{
			String[] values = new String[] {"False", "True"};
			ArrayList list = new ArrayList();
			int ordinal = 0;
			RolapMember root = null;
			RolapLevel level = (RolapLevel) hierarchy.getLevels()[0];
			if (hierarchy.hasAll()) {
				root = new RolapMember(
					null, level, null, hierarchy.getAllMemberName());
				root.ordinal = ordinal++;
				list.add(root);
				level = (RolapLevel) hierarchy.getLevels()[1];
			}
			for (int i = 0; i < values.length; i++) {
				RolapMember member = new RolapMember(root, level, values[i]);
				member.ordinal = ordinal++;
				list.add(member);
			}
			return (RolapMember[]) list.toArray(RolapUtil.emptyMemberArray);
		}
	}
}

// End ArrayMemberSource.java
