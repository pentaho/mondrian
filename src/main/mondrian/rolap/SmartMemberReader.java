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
import mondrian.olap.Level;
import mondrian.olap.Util;

import java.util.*;
import java.lang.ref.SoftReference;

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
public class SmartMemberReader implements MemberReader, MemberCache
{
	private MemberReader source;
	/** Maps {@link RolapMember} to a {@link ChildrenList} of its children, and
	 * records usages.
	 * Locking strategy is to lock the parent SmartMemberReader. **/
	private HashMap mapMemberToChildren = new HashMap();
	/** Maps a {@link MemberKey} to a {@link SoftReference} to a
	 * {@link RolapMember}, and is used to ensure that there is at most one
	 * object representing a given member.
	 * The soft reference allows members to be forgotten.
	 * Locking strategy is to lock the parent SmartMemberReader. **/
	private HashMap mapKeyToMember = new HashMap();
	private List rootMembers;

	SmartMemberReader(MemberReader source)
	{
		this.source = source;
		if (!source.setCache(this)) {
			throw Util.newInternal(
					"MemberSource (" + source + ", " + source.getClass() +
					") does not support cache-writeback");
		}
	}

	// implement MemberReader
	public RolapHierarchy getHierarchy()
	{
		return source.getHierarchy();
	}

	// implement MemberSource
	public boolean setCache(MemberCache cache)
	{
		// we do not support cache writeback -- we must be masters of our
		// own cache
		return false;
	}

	// implement MemberCache
	public Object makeKey(RolapMember parent, Object key)
	{
		return new MemberKey(parent, key);
	}

	// implement MemberCache
	// synchronization: Must synchronize, because uses mapKeyToMember
	public synchronized RolapMember getMember(Object key)
	{
		SoftReference ref = (SoftReference) mapKeyToMember.get(key);
		if (ref == null) {
			return null;
		}
		final RolapMember rolapMember = (RolapMember) ref.get();
		if (rolapMember == null) {
			// Referenced member has been garbage collected; remove the hash
			// table entry too.
			mapKeyToMember.put(key,null);
		}
		return rolapMember;
	}

	// implement MemberCache
	// synchronization: Must synchronize, because modifies mapKeyToMember
	public synchronized Object putMember(Object key, RolapMember value)
	{
		return mapKeyToMember.put(key, new SoftReference(value));
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

	public List getRootMembers()
	{
		if (rootMembers == null) {
			rootMembers = source.getRootMembers();
		}
		return rootMembers;
	}

	public List getMembersInLevel(
			RolapLevel level, int startOrdinal, int endOrdinal) {
		return source.getMembersInLevel(level, startOrdinal, endOrdinal);
	}

	private void getMembersInLevel(
		ArrayList result, RolapLevel level, int startOrdinal, int endOrdinal) {
		final List membersInLevel = getMembersInLevel(level, startOrdinal, endOrdinal);
		result.addAll(membersInLevel);
	}

	public void getMemberChildren(RolapMember parentMember, List children)
	{
		ArrayList parentMembers = new ArrayList();
		parentMembers.add(parentMember);
		getMemberChildren(parentMembers, children);
	}

	// synchronization: Does not need to be synchronized. It doesn't matter if
	// 'missed' contains too many members.
	public void getMemberChildren(List parentMembers, List children) {
		ArrayList missed = new ArrayList();
		for (int i = 0, n = parentMembers.size(); i < n; i++) {
			RolapMember parent = (RolapMember) parentMembers.get(i);
			CachePool.SoftCacheableReference ref = (CachePool.SoftCacheableReference)
					mapMemberToChildren.get(parent);
			if (ref == null) {
				missed.add(parent);
			} else {
				ChildrenList v = (ChildrenList) ref.getCacheable();
				if (v == null) {
					missed.add(parent);
				} else {
					children.addAll(v.list);
				}
			}
		}
		if (missed.size() > 0) {
			readMemberChildren(missed, children);
		}
	}

	public RolapMember lookupMember(String[] uniqueNameParts, boolean failIfNotFound) {
		return RolapUtil.lookupMember(this, uniqueNameParts, failIfNotFound);
	}

	/**
	 * A <code>ChildrenList</code> is held in the {@link #mapMemberToChildren}
	 * cache. It implements {@link CachePool.Cacheable}, so it can be removed
	 * if it is not pulling its weight.
	 *
	 * <p><b>Note to developers</b>: this class must obey the contract for
	 * objects which implement {@link CachePool.Cacheable}.
	 **/
	private static class ChildrenList implements CachePool.Cacheable
	{
		private SmartMemberReader reader;
		private RolapMember member;
		private double recency;
		private int pinCount;
		private ArrayList list;

		ChildrenList(
				SmartMemberReader reader, RolapMember member, ArrayList list) {
			this.reader = reader;
			this.member = member;
			this.list = list;
		}

		public String toString() {
			return super.toString() + " {member=" + member + ", childCount=" + list.size() + "}";
		}

		protected synchronized void finalize() {
			CachePool.instance().deregister(this, true); // per Cacheable contract
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
			reader.remove(this);
		}
		// implement CachePool.Cacheable
		public void markAccessed(double recency)
		{
			this.recency = recency;
		}
		// implement CachePool.Cacheable
		public void setPinCount(int pinCount)
		{
			System.out.println("SmartMemberReader: pinCount=" + pinCount + " (was " + this.pinCount + ")");
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
	 *
	 * @param result Children are written here, in order
	 * @param members Members whose children to read
	 * @pre isSorted(members)
	 **/
	private void readMemberChildren(List members, List result) {
		Util.assertPrecondition(isSorted(members), "isSorted(members)");
		ArrayList children = new ArrayList();
		source.getMemberChildren(members, children);
		// Put them in a temporary hash table first. Register them later, when
		// we know their size (hence their 'cost' to the cache pool).
		HashMap tempMap = new HashMap();
		for (int i = 0, n = members.size(); i < n; i++) {
			tempMap.put(members.get(i), new ArrayList());
		}
		for (int i = 0, childrenCount = children.size(); i < childrenCount; i++) {
			// todo: We could optimize here. If members.length is small, it's
			// more efficient to drive from members, rather than hashing
			// children.length times. We could also exploit the fact that the
			// result is sorted by ordinal and therefore, unless the "members"
			// contains members from different levels, children of the same
			// member will be contiguous.
			RolapMember child = (RolapMember) children.get(i);
			ArrayList list = (ArrayList) tempMap.get(child.getParentMember());
			list.add(child);
			result.add(child);
		}
		synchronized (this) {
			for (Iterator keys = tempMap.keySet().iterator(); keys.hasNext();) {
				RolapMember member = (RolapMember) keys.next();
				ArrayList list = (ArrayList) tempMap.get(member);
				if (!hasChildren(member)) {
					putChildren(member, list);
				}
			}
		}
	}

	/**
	 * Returns true if every element of <code>members</code> is not null and is
	 * strictly less than the following element; false otherwise.
	 */
	public boolean isSorted(List members) {
		final int count = members.size();
		if (count > 0 && members.get(0) == null) {
			// Special case check for 0th element, just in case length == 1.
			return false;
		}
		for (int i = 1; i < count; i++) {
			RolapMember m0 = (RolapMember) members.get(i - 1),
					m1 = (RolapMember) members.get(i);
			if (m1 == null ||
					compare(m0, m1, false) >= 0) {
				return false;
			}
		}
		return true;
	}

	// synchronization: Must synchronize, because uses mapMemberToChildren
	public synchronized boolean hasChildren(RolapMember member) {
		return mapMemberToChildren.get(member) != null;
	}

	// synchronization: Must synchronize, because modifies mapMemberToChildren.
	// But locking protocol dictates that we lock CachePool before we lock this.
	public void putChildren(RolapMember member, ArrayList children) {
		ChildrenList childrenList = new ChildrenList(this, member, children);
		CachePool.SoftCacheableReference ref = new CachePool.SoftCacheableReference(childrenList);
		final CachePool cachePool = CachePool.instance();
		synchronized (cachePool) {
			if (cachePool.isRegistered(childrenList)) {
				if (RolapUtil.debugOut != null) {
					RolapUtil.debugOut.println("SmartMemberReader: putChildren: " +
							childrenList + "was already in the cache " +
							" (not an error, but a rare race condition)");
				}
			} else {
				synchronized (this) {
					CachePool.SoftCacheableReference oldRef =
							(CachePool.SoftCacheableReference) mapMemberToChildren.put(member, ref);
					if (oldRef != null) {
						ChildrenList old = (ChildrenList) oldRef.getCacheableOrFail();
                        if (RolapUtil.debugOut != null) {
                            RolapUtil.debugOut.println("putChildren: remove " + oldRef + ", " + old);
                        }
                        // Temporarily put the old list back, while we
                        // deregister it.
                        mapMemberToChildren.put(member, oldRef);
						cachePool.deregister(old, false);
                        mapMemberToChildren.put(member, ref);
					}
					cachePool.register(childrenList);
				}
			}
		}
	}

	// synchronization: Must synchronize, because modifies mapMemberToChildren
	private synchronized void remove(ChildrenList childrenList) {
		CachePool.SoftCacheableReference ref =
				(CachePool.SoftCacheableReference)
				mapMemberToChildren.remove(childrenList.member);
		if (ref == null || !ref.refersTo(childrenList)) {
			throw Util.newInternal(
				"removeFromCache: ChildrenList " + childrenList +
				" is not registered with its SmartMemberReader (ref=" +
				ref + ")");
		}
	}

	// synchronization: Must synchronize, because uses mapMemberToChildren
	public RolapMember getLeadMember(RolapMember member, int n)
	{
		if (n == 0) {
			return member;
		} else if (false) {
			RolapMember parent = (RolapMember) member.getParentMember();
			CachePool.SoftCacheableReference refSiblings;
			synchronized (this) {
				refSiblings = (CachePool.SoftCacheableReference)
						mapMemberToChildren.get(parent);
			}
			if (refSiblings != null) {
				ChildrenList siblings = (ChildrenList)
						refSiblings.getCacheableOrFail();
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

	public void getMemberRange(
			RolapLevel level, RolapMember startMember, RolapMember endMember, List list) {
		Util.assertPrecondition(startMember != null, "startMember != null");
		Util.assertPrecondition(endMember != null, "endMember != null");
		Util.assertPrecondition(startMember.getLevel() == endMember.getLevel(),
				"startMember.getLevel() == endMember.getLevel()");
		if (compare(startMember, endMember, false) > 0) {
			return;
		}
		list.add(startMember);
		if (startMember == endMember) {
			return;
		}
		SiblingIterator siblings = new SiblingIterator(this, startMember);
		while (siblings.hasNext()) {
			final RolapMember member = siblings.nextMember();
			list.add(member);
			if (member == endMember) {
				return;
			}
		}
		throw Util.newInternal("sibling iterator did not hit end point, start=" +
				startMember + ", end=" + endMember);
	}

	public void _getMemberRange(
			RolapLevel level, RolapMember startMember, RolapMember endMember, List list) {
		// todo: Use a more efficient algorithm, which makes less use of
		// bounds.
		Util.assertTrue(level == startMember.getLevel());
		Util.assertTrue(level == endMember.getLevel());
		// "m" is the lowest member which is an ancestor of both "startMember"
		// and "endMember". If "startMember" == "endMember", then "m" is
		// "startMember".
		RolapMember m = endMember;
		while (m != null) {
			if (compare(m, startMember, false) <= 0) {
				break;
			}
			m = (RolapMember) m.getParentMember();
		}
		_getDescendants(m, startMember.getLevel(), startMember, endMember, list);
	}

	/**
	 * Returns the descendants of <code>member</code> at <code>level</code>
	 * whose ordinal is between <code>startOrdinal</code> and
	 * <code>endOrdinal</code>.
	 **/
	private void _getDescendants(
			RolapMember member, Level level, RolapMember startMember,
			RolapMember endMember, List result) {
		// todo: Make algortihm more efficient: Use binary search.
		List members = new ArrayList();
		members.add(member);
		while (true) {
			ArrayList children = new ArrayList();
			getMemberChildren(members, children);
			int count = children.size(),
					start,
					end;
			if (startMember == null) {
				start = 0;
			} else {
				start = count;
				for (int i = 0; i < count; i++) {
					final RolapMember m = (RolapMember) children.get(i);
					if (compare(m, startMember, false) >= 0) {
						start = i;
						break;
					}
				}
			}
			if (endMember == null) {
				end = count;
			} else {
				end = count;
				for (int i = start; i < count; i++) {
					final RolapMember m = (RolapMember) children.get(i);
					if (compare(m, endMember, false) >= 0) {
						end = i;
						break;
					}
				}
			}
			List trimmedChildren = children.subList(start, end);
			if (trimmedChildren.isEmpty() ||
					((RolapMember) children.get(0)).getLevel() == level) {
				result.addAll(trimmedChildren);
				return;
			}
			members = trimmedChildren;
		}
	}

	public int getMemberCount()
	{
		return source.getMemberCount();
	}

	public int compare(RolapMember m1, RolapMember m2, boolean siblingsAreEqual) {
		if (m1 == m2) {
			return 0;
		}
		if (m1.getParentMember() == m2.getParentMember()) {
			// including case where both parents are null
			if (siblingsAreEqual) {
				return 0;
			} else {
				ArrayList children = new ArrayList();
				getMemberChildren((RolapMember) m1.getParentMember(), children);
				int pos1 = -1, pos2 = -1;
				for (int i = 0, n = children.size(); i < n; i++) {
					RolapMember child = (RolapMember) children.get(i);
					if (child == m1) {
						pos1 = i;
					}
					if (child == m2) {
						pos2 = i;
					}
				}
				if (pos1 == -1) {
					throw Util.newInternal(m1 + " not found among siblings");
				}
				if (pos2 == -1) {
					throw Util.newInternal(m2 + " not found among siblings");
				}
				Util.assertTrue(pos1 != pos2);
				return pos1 < pos2 ? -1 : 1;
			}
		}
		int levelDepth1 = m1.getLevel().getDepth(),
			levelDepth2 = m2.getLevel().getDepth();
		if (levelDepth1 < levelDepth2) {
			final int c = compare(m1, (RolapMember) m2.getParentMember(), false);
			return (c == 0) ? -1 : c;
		} else if (levelDepth1 > levelDepth2) {
			final int c = compare((RolapMember) m1.getParentMember(), m2, false);
			return (c == 0) ? 1 : c;
		} else {
			return compare((RolapMember) m1.getParentMember(), (RolapMember) m2.getParentMember(), false);
		}
	}
}

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
		List siblingList;
		if (parent == null) {
			siblingList = reader.getRootMembers();
		} else {
			siblingList = new ArrayList();
			reader.getMemberChildren(parent, siblingList);
			this.parentIterator = new SiblingIterator(reader, parent);
		}
		this.siblings = RolapUtil.toArray(siblingList);
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
			ArrayList siblingList = new ArrayList();
			reader.getMemberChildren(parent, siblingList);
			this.siblings = RolapUtil.toArray(siblingList);
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
			ArrayList siblingList = new ArrayList();
			reader.getMemberChildren(parent, siblingList);
			this.siblings = RolapUtil.toArray(siblingList);
			this.position = this.siblings.length - 1;
		}
		return this.siblings[this.position];
	}
}

// End SmartMemberReader.java
