/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 December, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.rolap.sql.SqlQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.WeakHashMap;
import java.util.Iterator;

/**
 * <code>SmartMemberReader</code> implements {@link MemberReader} by keeping a
 * cache of members and their children. If a member is 'in cache', there is a
 * list of its children. Its children are not necessarily 'in cache'.
 *
 * <p>Cache retention. We have not implemented a cache flushing policy. Members
 * are never removed from the cache.</p>
 *
 * <p>Uniqueness. We need to ensure that there is never more than one {@link
 * RolapMember} object representing the same member.</p>
 *
 * @author jhyde
 * @since 21 December, 2001
 * @version $Id$
 **/
class SmartMemberReader implements MemberReader, MemberCache
{
	private MemberReader source;
	/** Maps {@link RolapMember} to a {@link Vector} of its children, and
	 * records usages. **/
	private HashMap mapMemberToChildren;
	/** Maps a {@link MemberKey} to a {@link RolapMember}. Implemented using a
	 * {@link WeakHashMap} so that members can be forgotten. **/
	private WeakHashMap mapKeyToMember;
	private RolapMember[] rootMembers;

	SmartMemberReader(MemberReader source)
	{
		this.source = source;
		source.setCache(this);
		mapMemberToChildren = new HashMap();
		mapKeyToMember = new WeakHashMap();
	}

	// implement MemberReader
	public RolapHierarchy getHierarchy()
	{
		return source.getHierarchy();
	}

	// implement MemberSource
	public void setCache(MemberCache cache)
	{
		throw Util.newInternal(
			getClass() + " must be master of its own cache");
	}

	// implement MemberCache
	public Object makeKey(RolapMember parent, Object key)
	{
		return new MemberKey(parent, key);
	}

	// implement MemberCache
	public RolapMember getMember(Object key)
	{
		return (RolapMember) mapKeyToMember.get(key);
	}

	// implement MemberCache
	public Object putMember(Object key, RolapMember value)
	{
		return mapKeyToMember.put(key, value);
	}

	// implement MemberReader
	public RolapMember[] getMembers()
	{
		ArrayList v = new ArrayList();
		RolapLevel[] levels = (RolapLevel[]) getHierarchy().getLevels();
		// todo: optimize by walking to children for members we know about
		for (int i = 0; i < levels.length; i++) {
			getMembersInLevel(v, levels[i], 0, Integer.MAX_VALUE);
		}
		return RolapUtil.toArray(v);
	}

	public RolapMember[] getRootMembers()
	{
		if (rootMembers == null) {
			rootMembers = source.getRootMembers();
		}
		return rootMembers;
	}

	// implement MemberSource
	public RolapMember[] getMembersInLevel(
		RolapLevel level, int startOrdinal, int endOrdinal)
	{
		return source.getMembersInLevel(level, startOrdinal, endOrdinal);
	}

	private void getMembersInLevel(
		ArrayList result, RolapLevel level, int startOrdinal, int endOrdinal)
	{
		// todo: change the MemberSource interface
		RolapMember[] members = getMembersInLevel(
			level, startOrdinal, endOrdinal);
		RolapUtil.add(result, members);
	}

	public RolapMember[] getMemberChildren(RolapMember[] parentMembers)
	{
		ArrayList result = new ArrayList();
		getMemberChildren(result, parentMembers);
		return RolapUtil.toArray(result);
	}

	private void getMemberChildren(
		ArrayList result, RolapMember[] parentMembers)
	{
		ArrayList missed = new ArrayList();
		for (int i = 0; i < parentMembers.length; i++) {
			RolapMember parent = parentMembers[i];
			ChildrenList v = (ChildrenList) mapMemberToChildren.get(parent);
			if (v == null) {
				missed.add(parent);
			} else {
				result.addAll(v.list);
			}
		}
		if (missed.size() > 0) {
			readMemberChildren(result, RolapUtil.toArray(missed));
		}
	}

	/**
	 * A <code>ChildrenList</code> is held in the {@link mapMemberToChildren}
	 * cache. It implements {@link CachePool.Cacheable}, so it can be removed
	 * if it is not pulling its weight.
	 **/
	static class ChildrenList implements CachePool.Cacheable
	{
		private SmartMemberReader reader;
		private RolapMember member;
		private double recency;
		private int pinCount;
		private ArrayList list;

		ChildrenList(SmartMemberReader reader, RolapMember member)
		{
			this.reader = reader;
			this.member = member;
			this.list = new ArrayList();
		}
		// implement CachePool.Cacheable
		public double getCost()
		{
			// assume 4 bytes for slot, 4 bytes for value
			return 8;
		}
		// implement CachePool.Cacheable
		public double getScore()
		{
			double benefit = getBenefit(),
				cost = getCost();
			return benefit / cost * recency;
		}
		// implement CachePool.Cacheable
		public void removeFromCache()
		{
			reader.mapMemberToChildren.remove(member);
		}
		// implement CachePool.Cacheable
		public void markAccessed(double recency)
		{
			this.recency = recency;
		}
		// implement CachePool.Cacheable
		public void setPinCount(int pinCount)
		{
			this.pinCount = pinCount;
		}
		// implement CachePool.Cacheable
		public int getPinCount()
		{
			return pinCount;
		}
		private double getBenefit()
		{
			return 2;
		}
	};

	/**
	 * Reads the children of <code>member</code> into cache, and also into
	 * <code>result</code>.
	 **/
	private void readMemberChildren(ArrayList result, RolapMember[] members)
	{
		RolapMember[] children = source.getMemberChildren(members);
		// Put them in a temporay hash table first. Register them later, when
		// we know their size (hence their 'cost' to the cache pool).
		HashMap tempMap = new HashMap();
		for (int i = 0; i < members.length; i++) {
			tempMap.put(members[i], new ChildrenList(this, members[i]));
		}
		for (int i = 0; i < children.length; i++) {
			// todo: We could optimize here. If members.length is small, it's
			// more efficient to drive from members, rather than hashing
			// children.length times. We could also exploit the fact that the
			// result is sorted by ordinal and therefore, unless the "members"
			// contains members from different levels, children of the same
			// member will be contiguous.
			RolapMember child = children[i];
			ChildrenList v2 = (ChildrenList) tempMap.get(
				(RolapMember) child.getParentMember());
			v2.list.add(child);
			result.add(child);
		}
		CachePool pool = CachePool.instance();
		for (Iterator elements =
				 tempMap.values().iterator(); elements.hasNext(); ) {
			ChildrenList list = (ChildrenList) elements.next();
			pool.register(list);
		}
		mapMemberToChildren.putAll(tempMap);
	}

	public RolapMember getLeadMember(RolapMember member, int n)
	{
		if (n == 0) {
			return member;
		} else if (false) {
			RolapMember parent = (RolapMember) member.getParentMember();
			ChildrenList siblings = (ChildrenList)
				mapMemberToChildren.get(parent);
			if (siblings != null) {
				int pos = siblings.list.indexOf(member);
				Util.assertTrue(pos >= 0);
				int siblingPos = pos + n;
				if (siblingPos >= 0 && siblingPos < siblings.list.size()) {
					return (RolapMember) siblings.list.get(pos + n);
				}
			}
			return source.getLeadMember(member, n);
		} else {
			SiblingIterator iter = new SiblingIterator(this, member);
			if (n > 0) {
				RolapMember sibling = null;
				while (n-- > 0) {
					if (!iter.hasNext()) {
						return (RolapMember) member.getHierarchy().getNullMember();
					}
					sibling = iter.nextMember();
				}
				return sibling;
			} else {
				n = -n;
				RolapMember sibling = null;
				while (n-- > 0) {
					if (!iter.hasPrevious()) {
						return (RolapMember) member.getHierarchy().getNullMember();
					}
					sibling = iter.previousMember();
				}
				return sibling;
			}
		}
	}

//  	interface RolapMemberPredicate
//  	{
//  		boolean test(RolapMember member);
//  	};

//  	class OrdinalPredicate implements RolapMemberPredicate
//  	{
//  		OrdinalPredicate(int startOrdinal, int endOrdinal, RolapLevel level)
//  		{
//  			this.startOrdinal = startOrdinal;
//  			this.endOrdinal = endOrdinal;
//  			this.level = level;
//  		}
//  	};

	public RolapMember[] getPeriodsToDate(RolapLevel level, RolapMember member)
	{
		int startOrdinal = -1;
		RolapMember m = member;
		while (m != null) {
			if (m.getLevel() == level) {
				startOrdinal = m.ordinal;
				break;
			}
			m = (RolapMember) m.getParentMember();
		}
		if (startOrdinal == -1) {
			return new RolapMember[0]; // level not found
		}
		int endOrdinal = member.ordinal + 1;
		return getDescendants(m, level, startOrdinal, endOrdinal);
	}

	/**
	 * Returns the descendants of <code>member</code> at <code>level</code>
	 * whose ordinal is between <code>startOrdinal</code> and
	 * <code>endOrdinal</code>.
	 **/
	private RolapMember[] getDescendants(
		RolapMember member, Level level, int startOrdinal, int endOrdinal)
	{
		RolapMember[] members = new RolapMember[] {member};
		while (true) {
			ArrayList children = new ArrayList();
			getMemberChildren(children, members);
			RolapMember[] childrenArray = RolapUtil.toArray(children);
			int count = childrenArray.length, start = count, end = count;
			for (int i = 0; i < count; i++) {
				if (childrenArray[i].ordinal >= startOrdinal) {
					start = i;
					break;
				}
			}
			for (int i = start; i < count; i++) {
				if (childrenArray[i].ordinal >= endOrdinal) {
					end = i;
					break;
				}
			}
			members = new RolapMember[end - start];
			System.arraycopy(childrenArray, start, members, 0, end - start);
			if (members.length == 0 ||
				members[0].getLevel() == level) {
				return members;
			}
		}
	}

	public int getMemberCount()
	{
		return source.getMemberCount();
	}

	// implement MemberReader
	public void qualifyQuery(
		SqlQuery sqlQuery, RolapMember member)
	{
		source.qualifyQuery(sqlQuery, member);
	}
};

class SiblingIterator //implements Iterator
{
	MemberReader reader;
	SiblingIterator parentIterator;
	RolapMember[] siblings;
	int position;

	SiblingIterator(MemberReader reader, RolapMember member)
	{
		this.reader = reader;
		RolapMember parent = (RolapMember) member.getParentMember();
		if (parent == null) {
			this.siblings = reader.getRootMembers();
		} else {
			this.siblings = reader.getMemberChildren(
				new RolapMember[] {parent});
			this.parentIterator = new SiblingIterator(reader, parent);
		}
		this.position = -1;
		for (int i = 0; i < this.siblings.length; i++) {
			if (siblings[i] == member) {
				this.position = i;
				break;
			}
		}
		if (this.position == -1) {
			throw Util.newInternal(
				"member " + member + " not found among its siblings");
		}
	}
	boolean hasNext()
	{
		return this.position < this.siblings.length - 1 ||
			parentIterator != null &&
			parentIterator.hasNext();
	}
	Object next()
	{
		return nextMember();
	}
	RolapMember nextMember()
	{
		if (++this.position >= this.siblings.length) {
			if (parentIterator == null) {
				throw Util.newInternal("there is no next member");
			}
			RolapMember parent = parentIterator.nextMember();
			this.siblings = reader.getMemberChildren(
				new RolapMember[] {parent});
			this.position = 0;
		}
		return this.siblings[this.position];
	}
	boolean hasPrevious()
	{
		return this.position > 0 ||
			parentIterator != null &&
			parentIterator.hasPrevious();
	}
	Object previous()
	{
		return previousMember();
	}
	RolapMember previousMember()
	{
		if (--this.position < 0) {
			if (parentIterator == null) {
				throw Util.newInternal("there is no next member");
			}
			RolapMember parent = parentIterator.previousMember();
			this.siblings = reader.getMemberChildren(
				new RolapMember[] {parent});
			this.position = this.siblings.length - 1;
		}
		return this.siblings[this.position];
	}
};

// End SmartMemberReader.java
