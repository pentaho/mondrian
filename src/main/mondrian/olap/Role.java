/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Oct 5, 2002
*/

package mondrian.olap;

import java.util.HashMap;
import java.util.Iterator;

/**
 * A <code>Role</code> is a collection of access rights to cubes, permissions,
 * and so forth.
 *
 * <p>At present, the only way to create a role is programmatically. You then
 * add appropriate permissions, and associate the role with a connection.
 * Queries executed for the duration of the connection will b.
 *
 * <p>Mondrian does not have any notion of a 'user'. It is the client
 * application's responsibility to create a role appropriate for the user who
 * is establishing the connection.
 *
 * @see mondrian.test.AccessControlTest
 *
 * @author jhyde
 * @since Oct 5, 2002
 * @version $Id$
 **/
public class Role {
	private boolean mutable = true;
	/** Maps {@link Schema} to {@link Boolean},
	 * {@link Cube} to {@link Boolean},
	 * {@link Dimension} to {@link Boolean},
	 * {@link Hierarchy} to {@link HierarchyAccess}. */
	private HashMap grants = new HashMap();

	/**
	 * Creates a role with no permissions.
	 */
	public Role() {}

	protected Object clone() {
		Role role = new Role();
		role.mutable = mutable;
		role.grants.putAll(grants);
		for (Iterator iter = grants.values().iterator(); iter.hasNext();) {
			Object value = iter.next();
			if (value instanceof HierarchyAccess) {
				final HierarchyAccess hierarchyAccess = (HierarchyAccess) value;
				role.grants.put(hierarchyAccess.hierarchy, hierarchyAccess.clone());
			}
		}
		return role;
	}

	/**
	 * Returns a copy of this <code>Role</code> which can be modified.
	 */
	public Role makeMutableClone() {
		Role role = (Role) clone();
		role.mutable = true;
		return role;
	}

	/**
	 * Prevents any further modifications.
	 * @post !isMutable()
	 */
	public void makeImmutable() {
		mutable = false;
	}
	/**
	 * Returns whether modifications are possible.
	 */
	public boolean isMutable() {
		return mutable;
	}
	/**
	 * Defines access to all cubes and dimensions in a schema.
	 *
	 * @param schema Schema whose access to grant/deny.
	 * @param access An {@link Access access code}
	 *
	 * @pre schema != null
	 * @pre access == Access.ALL || access == Access.NONE
	 * @pre isMutable()
	 */
	public void grant(Schema schema, int access) {
		Util.assertPrecondition(schema != null, "schema != null");
		Util.assertPrecondition(access == Access.ALL || access == Access.NONE, "access == Access.ALL || access == Access.NONE");
		Util.assertPrecondition(isMutable(), "isMutable()");
		grants.put(schema, toBoolean(access));
	}

	/**
	 * Returns the access this role has to a given schema.
	 *
	 * @pre schema != null
	 * @post return == Access.ALL || return == Access.NONE
	 */
	public int getAccess(Schema schema) {
		Util.assertPrecondition(schema != null, "schema != null");
		Boolean b = (Boolean) grants.get(schema);
		return toAccess(b);
	}

	private static Boolean toBoolean(int access) {
		return access == Access.ALL ? Boolean.TRUE : Boolean.FALSE;
	}

	private static int toAccess(Boolean b) {
		return b != null && b.booleanValue() ? Access.ALL :
				Access.NONE;
	}

	/**
	 * Defines access to a cube.
	 *
	 * @param cube Cube whose access to grant/deny.
	 * @param access An {@link Access access code}
	 *
	 * @pre cube != null
	 * @pre access == Access.ALL || access == Access.NONE
	 * @pre isMutable()
	 */
	public void grant(Cube cube, int access) {
		Util.assertPrecondition(cube != null, "cube != null");
		Util.assertPrecondition(access == Access.ALL || access == Access.NONE, "access == Access.ALL || access == Access.NONE");
		Util.assertPrecondition(isMutable(), "isMutable()");
		grants.put(cube, toBoolean(access));
	}

	/**
	 * Returns the access this role has to a given cube.
	 *
	 * @pre cube != null
	 * @post return == Access.ALL || return == Access.NONE
	 */
	public int getAccess(Cube cube) {
		Util.assertPrecondition(cube != null, "cube != null");
		Boolean access = (Boolean) grants.get(cube);
		if (access == null) {
			access = (Boolean) grants.get(cube.getSchema());
		}
		return toAccess(access);
	}

	private static class HierarchyAccess {
		private Hierarchy hierarchy;
		private Level topLevel;
		private int access;
		private Level bottomLevel;
		private HashMap memberGrants = new HashMap();

		HierarchyAccess(Hierarchy hierarchy, int access, Level topLevel, Level bottomLevel) {
			this.hierarchy = hierarchy;
			this.access = access;
			this.topLevel = topLevel;
			this.bottomLevel = bottomLevel;
		}

		public Object clone() {
			HierarchyAccess hierarchyAccess = new HierarchyAccess(
					hierarchy, access, topLevel, bottomLevel);
			hierarchyAccess.memberGrants.putAll(memberGrants);
			return hierarchyAccess;
		}

		void grant(Member member, int access) {
			Util.assertTrue(member.getHierarchy() == hierarchy);
			// Remove any existing grants to descendants of "member"
			for (Iterator membersIter = memberGrants.keySet().iterator();
				 membersIter.hasNext();) {
				Member m = (Member) membersIter.next();
				if (m.isChildOrEqualTo(member)) {
					membersIter.remove();
				}
			}
			memberGrants.put(member, toBoolean(access));
		}
	}

	/**
	 * Defines access to a dimension.
	 *
	 * @param dimension Hierarchy whose access to grant/deny.
	 * @param access An {@link Access access code}
	 *
	 * @pre dimension != null
	 * @pre access == Access.ALL || access == Access.NONE
	 * @pre isMutable()
	 */
	public void grant(Dimension dimension, int access) {
		Util.assertPrecondition(dimension != null, "dimension != null");
		Util.assertPrecondition(access == Access.ALL || access == Access.NONE, "access == Access.ALL || access == Access.NONE");
		Util.assertPrecondition(isMutable(), "isMutable()");
		grants.put(dimension, toBoolean(access));
	}

	/**
	 * Returns the access this role has to a given dimension.
	 *
	 * @pre dimension != null
	 * @post Access.instance().isValid(return)
	 */
	public int getAccess(Dimension dimension) {
		Util.assertPrecondition(dimension != null, "dimension != null");
		Boolean b = (Boolean) grants.get(dimension);
		if (b != null) {
			return toAccess(b);
		}
		return getAccess(dimension.getSchema());
	}

	/**
	 * Defines access to a hierarchy.
	 *
	 * @param hierarchy Hierarchy whose access to grant/deny.
	 * @param access An {@link Access access code}
	 * @param topLevel Top-most level which can be accessed, or null if the
	 *     highest level. May only be specified if <code>access</code> is
	 *    {@link Access#CUSTOM}.
	 * @param bottomLevel Bottom-most level which can be accessed, or null if
	 *     the lowest level. May only be specified if <code>access</code> is
	 *    {@link Access#CUSTOM}.
	 *
	 * @pre hierarchy != null
	 * @pre Access.instance().isValid(access)
	 * @pre (access == Access.CUSTOM) || (topLevel == null && bottomLevel == null)
	 * @pre topLevel == null || topLevel.getHierarchy() == hierarchy
	 * @pre bottomLevel == null || bottomLevel.getHierarchy() == hierarchy
	 * @pre isMutable()
	 */
	public void grant(
			Hierarchy hierarchy, int access, Level topLevel,
			Level bottomLevel) {
		Util.assertPrecondition(hierarchy != null, "hierarchy != null");
		Util.assertPrecondition(Access.instance().isValid(access));
		Util.assertPrecondition((access == Access.CUSTOM) || (topLevel == null && bottomLevel == null), "access == Access.CUSTOM) || (topLevel == null && bottomLevel == null)");
		Util.assertPrecondition(topLevel == null || topLevel.getHierarchy() == hierarchy, "topLevel == null || topLevel.getHierarchy() == hierarchy");
		Util.assertPrecondition(bottomLevel == null || bottomLevel.getHierarchy() == hierarchy, "bottomLevel == null || bottomLevel.getHierarchy() == hierarchy");
		Util.assertPrecondition(isMutable(), "isMutable()");
		grants.put(hierarchy, new HierarchyAccess(hierarchy, access, topLevel, bottomLevel));
	}

	/**
	 * Returns the access this role has to a given hierarchy.
	 *
	 * @pre hierarchy != null
	 * @post Access.instance().isValid(return)
	 */
	public int getAccess(Hierarchy hierarchy) {
		Util.assertPrecondition(hierarchy != null, "hierarchy != null");
		HierarchyAccess access = (HierarchyAccess) grants.get(hierarchy);
		if (access != null) {
			return access.access;
		}
		return getAccess(hierarchy.getDimension());
	}

	/**
	 * Returns the access this role has to a given level.
	 *
	 * @pre level != null
	 * @post Access.instance().isValid(return)
	 */
	public int getAccess(Level level) {
		Util.assertPrecondition(level != null, "level != null");
		HierarchyAccess access = (HierarchyAccess) grants.get(level.getHierarchy());
		if (access != null) {
			if (access.topLevel != null &&
					level.getDepth() < access.topLevel.getDepth()) {
				return Access.NONE;
			}
			if (access.bottomLevel != null &&
					level.getDepth() > access.bottomLevel.getDepth()) {
				return Access.NONE;
			}
			return access.access;
		}
		return getAccess(level.getDimension().getSchema());
	}

	/**
	 * Defines access to a member in a hierarchy.
	 *
	 * <p>Notes:<ol>
	 * <li>The order of grants matters. If you grant/deny access to a
	 *     member, previous grants/denials to its descendants are ignored.</li>
	 * <li>Member grants do not supersde top/bottom levels set using {@link
	 *     #grant(Hierarchy,int,Level,Level)}.
	 * <li>If you have access to a member, then you can see its ancestors
	 *     <em>even those explicitly denied</em>, up to the top level.
	 * </ol>
	 *
	 * @pre member != null
	 * @pre Access.instance().isValid(access)
	 * @pre isMutable()
	 * @pre getAccess(member.getHierarchy()) == Access.CUSTOM
	 */
	public void grant(Member member, int access) {
		Util.assertPrecondition(member != null, "member != null");
		Util.assertPrecondition(Access.instance().isValid(access), "Access.instance().isValid(access)");
		Util.assertPrecondition(isMutable(), "isMutable()");
		Util.assertPrecondition(getAccess(member.getHierarchy()) == Access.CUSTOM, "getAccess(member.getHierarchy()) == Access.CUSTOM");
		HierarchyAccess hierarchyAccess = (HierarchyAccess) grants.get(member.getHierarchy());
		Util.assertTrue(hierarchyAccess != null && hierarchyAccess.access == Access.CUSTOM);
		hierarchyAccess.grant(member, access);
	}

	/**
	 * Returns the access this role has to a given member.
	 *
	 * @pre member != null
	 * @pre isMutable()
	 * @post Access.instance().isValid(return)
	 */
	public int getAccess(Member member) {
		Util.assertPrecondition(member != null, "member != null");
		HierarchyAccess hierarchyAccess = (HierarchyAccess)
				grants.get(member.getHierarchy());
		int access = getAccess(member.getDimension().getSchema());
		if (hierarchyAccess != null) {
			for (Iterator membersIter = hierarchyAccess.memberGrants.keySet().iterator(); membersIter.hasNext();) {
				Member m = (Member) membersIter.next();
				if (member.isChildOrEqualTo(m)) {
					access = hierarchyAccess.access;
				}
			}
		}
		return access;
	}

	/**
	 * Returns whether this role is allowed to see a given element.
	 * @pre olapElement != null
	 */
	public boolean canAccess(OlapElement olapElement) {
		Util.assertPrecondition(olapElement != null, "olapElement != null");
		if (olapElement instanceof Cube) {
			return getAccess((Cube) olapElement) != Access.NONE;
		} else if (olapElement instanceof Dimension) {
			return getAccess((Dimension) olapElement) != Access.NONE;
		} else if (olapElement instanceof Hierarchy) {
			return getAccess((Hierarchy) olapElement) != Access.NONE;
		} else if (olapElement instanceof Level) {
			return getAccess((Level) olapElement) != Access.NONE;
		} else if (olapElement instanceof Member) {
			return getAccess((Member) olapElement) != Access.NONE;
		} else {
			return false;
		}
	}
}

// End Role.java