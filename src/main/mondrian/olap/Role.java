/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Oct 5, 2002
*/

package mondrian.olap;

import junit.framework.TestCase;
import mondrian.test.FoodMartTestCase;

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
 * @author jhyde
 * @since Oct 5, 2002
 * @version $Id$
 **/
public class Role {
	/**
	 * Creates a role with no permissions.
	 */
	public Role() {}

	public static final int ACCESS_ALL = 1;
	public static final int ACCESS_NONE = 2;
	public static final int ACCESS_CUSTOM = 3;

	/**
	 * Defines access to all cubes and dimensions in a schema.
	 *
	 * @param schema Schema whose access to grant/deny.
	 * @param access Access, one of: {@link #ACCESS_ALL}, {@link #ACCESS_NONE}.
	 *
	 * @pre schema != null
	 * @pre access == ACCESS_ALL || access == ACCESS_NONE
	 */
	public void grant(Schema schema, int access) {
	}

	/**
	 * Defines access to a cube.
	 *
	 * @param schema Cube whose access to grant/deny.
	 * @param access Access, one of: {@link #ACCESS_ALL}, {@link #ACCESS_NONE}.
	 *
	 * @pre cube != null
	 * @pre access == ACCESS_ALL || access == ACCESS_NONE
	 */
	public void grant(Cube cube, int access) {
	}

	/**
	 * Defines access to a hierarchy.
	 *
	 * @param schema Cube whose access to grant/deny.
	 * @param access Access, one of: {@link #ACCESS_ALL}, {@link #ACCESS_NONE},
	 *     {@link #ACCESS_CUSTOM}.
	 * @param topLevel Top-most level which can be accessed, or null if the
	 *     highest level. May only be specified if {@link #ACCESS_CUSTOM}.
	 * @param bottomLevel Bottom-most level which can be accessed, or null if
	 *     the lowest level. May only be specified if {@link #ACCESS_CUSTOM}.
	 *
	 * @pre hierarchy != null
	 * @pre access == ACCESS_ALL || access == ACCESS_NONE || access == ACCESS_CUSTOM
	 * @pre (access == ACCESS_CUSTOM) || (topLevel == null && bottomLevel == null)
	 * @pre topLevel == null || topLevel.getHierarchy() == hierarchy
	 * @pre bottomLevel == null || bottomLevel.getHierarchy() == hierarchy
	 */
	public void grant(
			Hierarchy hierarchy, int access, Level topLevel,
			Level bottomLevel) {
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
	 */
	public void grant(Member member, int access) {
	}

}

class TestRole extends FoodMartTestCase {
	public TestRole(String name) {
		super(name);
	}
	public void testGrantDimensionNone() {
		Connection connection = getConnection();
		Role role = new Role();
		connection.setRole(role);
		Schema schema = connection.getSchema();
		Cube salesCube = schema.lookupCube("Sales", true);
		// todo: add Schema.lookupDimension
		Hierarchy genderHierarchy = salesCube.lookupHierarchy("Gender", false);
		role.grant(genderHierarchy, Role.ACCESS_NONE, null, null);
		// todo: check that it works
	}
	public void testGrantHierarchyCustom() {
		Connection connection = getConnection();
		Role role = new Role();
		connection.setRole(role);
		Schema schema = connection.getSchema();
		final boolean fail = true;
		Cube salesCube = schema.lookupCube("Sales", fail);
		Hierarchy storeHierarchy = salesCube.lookupHierarchy("Store", false);
		Level nationLevel = storeHierarchy.lookupLevel("Nation");
		Level cityLevel = storeHierarchy.lookupLevel("City");
		role.grant(storeHierarchy, Role.ACCESS_CUSTOM, nationLevel, cityLevel);
		role.grant(storeHierarchy.lookupMemberByUniqueName("[Store].[USA].[OR]", fail), Role.ACCESS_ALL);
		role.grant(storeHierarchy.lookupMemberByUniqueName("[Store].[USA]", fail), Role.ACCESS_NONE);
		role.grant(storeHierarchy.lookupMemberByUniqueName("[Store].[San Francisco]", fail), Role.ACCESS_ALL);
		// assert: can access Mexico
		// assert: can access San Francisco
		// assert: can access USA (rule 3 - parent of allowed member San Francisco)
		// assert: can not access California
		// assert: can not access Oregon (rule 1 - order matters)
		// assert: can not access Washington (child of denied member)
		// assert: can not access All (above top level)
		// assert: can not access Fred in San Francisco (below bottom level)
		// todo: check that totals for USA include missing cells
	}
}

// End Role.java