/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 July, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.xom.XOMUtil;
import mondrian.xom.Parser;
import mondrian.xom.XOMException;
import mondrian.xom.DOMWrapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * A <code>RolapSchema</code> is a collection of {@link RolapCube}s and
 * shared {@link RolapDimension}s. It is shared betweeen {@link
 * RolapConnection}s. It caches {@link MemberReader}s, etc.
 *
 * @see RolapConnection
 * @author jhyde
 * @since 26 July, 2001
 * @version $Id$
 **/
public class RolapSchema implements Schema
{
    private String name;
	/**
	 * Internal use only.
	 */
	private RolapConnection internalConnection;
	/**
	 * Holds cubes in this schema.
	 */
	private final HashMap mapNameToCube = new HashMap();
	/**
	 * Maps {@link String shared hierarchy name} to {@link MemberReader}.
	 * Shared between all statements which use this connection.
	 */
	private final HashMap mapSharedHierarchyToReader = new HashMap();
	/**
	 * Contains {@link HierarchyUsage}s for all hierarchy and fact-table
	 * combinations.
	 **/
	private final HashMap hierarchyUsages = new HashMap();

	/**
	 * Maps {@link String names of shared hierarchies} to {@link
	 * RolapHierarchy the canonical instance of those hierarchies}.
	 */
	private final HashMap mapSharedHierarchyNameToHierarchy = new HashMap();
	/**
	 * The default role for connections to this schema.
	 */
	Role defaultRole = createDefaultRole();
	/**
	 * Maps {@link String names of roles} to {@link Role roles with those names}.
	 */
	private final HashMap mapNameToRole = new HashMap();
	private static final int[] schemaAllowed = new int[] {Access.NONE, Access.ALL, Access.ALL_DIMENSIONS};
	private static final int[] cubeAllowed = new int[] {Access.NONE, Access.ALL};
	private static final int[] dimensionAllowed = new int[] {Access.NONE, Access.ALL};
	private static final int[] hierarchyAllowed = new int[] {Access.NONE, Access.ALL, Access.CUSTOM};
	private static final int[] memberAllowed = new int[] {Access.NONE, Access.ALL};

	/**
	 * Creates a {@link RolapSchema}. Use
	 * <code>RolapSchema.Pool.instance().get(catalogName)</code>.
	 */
	private RolapSchema(String catalogName, Util.PropertyList connectInfo) {
		internalConnection = new RolapConnection(connectInfo, this);
		try {
			mondrian.xom.Parser xmlParser =
				mondrian.xom.XOMUtil.createDefaultParser();
			String schema = connectInfo.get(RolapConnectionProperties.CatalogContent);
			final DOMWrapper def;
			if (schema == null) {
				java.net.URL url = new java.net.URL(catalogName);
				def = xmlParser.parse(url);
			} else {
				def = xmlParser.parse(schema);
			}
			MondrianDef.Schema xmlSchema = new MondrianDef.Schema(def);
			load(xmlSchema);
		} catch (mondrian.xom.XOMException e) {
			throw Util.newError(e, "while parsing catalog " + catalogName);
		} catch (java.io.IOException e) {
			throw Util.newError(e, "while parsing catalog " + catalogName);
		}
	}

    public String getName() {
        Util.assertPostcondition(name != null, "return != null");
        Util.assertPostcondition(name.length() > 0, "return.length() > 0");
        return name;
    }

	private void load(MondrianDef.Schema xmlSchema)
	{
        this.name = xmlSchema.name;
        if (name == null || name.equals("")) {
            throw Util.newError("<Schema> name must be set");
        }
		for (int i = 0; i < xmlSchema.cubes.length; i++) {
			MondrianDef.Cube xmlCube = xmlSchema.cubes[i];
			RolapCube cube = new RolapCube(this, xmlSchema, xmlCube);
			mapNameToCube.put(xmlCube.name, cube);
		}
		for (int i = 0; i < xmlSchema.virtualCubes.length; i++) {
			MondrianDef.VirtualCube xmlVirtualCube = xmlSchema.virtualCubes[i];
			RolapCube cube = new RolapCube(this, xmlSchema, xmlVirtualCube);
			mapNameToCube.put(xmlVirtualCube.name, cube);
		}
		for (int i = 0; i < xmlSchema.roles.length; i++) {
			MondrianDef.Role xmlRole = xmlSchema.roles[i];
			Role role = createRole(xmlRole);
			mapNameToRole.put(xmlRole.name, role);
		}
		if (xmlSchema.defaultRole != null) {
			Role role = lookupRole(xmlSchema.defaultRole);
			if (role == null) {
				throw Util.newError("Role '" + xmlSchema.defaultRole + "' not found");
			}
			defaultRole = role;
		}
	}

	private Role createRole(MondrianDef.Role xmlRole) {
		Role role = new Role();
		for (int i = 0; i < xmlRole.schemaGrants.length; i++) {
			MondrianDef.SchemaGrant schemaGrant = xmlRole.schemaGrants[i];
			role.grant(this, getAccess(schemaGrant.access, schemaAllowed));
			for (int j = 0; j < schemaGrant.cubeGrants.length; j++) {
				MondrianDef.CubeGrant cubeGrant = schemaGrant.cubeGrants[j];
				Cube cube = (Cube) mapNameToCube.get(cubeGrant.cube);
				if (cube == null) {
					throw Util.newError("Unknown cube '" + cube + "'");
				}
				role.grant(cube, getAccess(cubeGrant.access, cubeAllowed));
				final SchemaReader schemaReader = cube.getSchemaReader(null);
				for (int k = 0; k < cubeGrant.dimensionGrants.length; k++) {
					MondrianDef.DimensionGrant dimensionGrant = cubeGrant.dimensionGrants[k];
					Dimension dimension = (Dimension) Util.lookupCompound(
							schemaReader, cube, Util.explode(dimensionGrant.dimension), true, Category.Dimension);
					role.grant(dimension, getAccess(dimensionGrant.access, dimensionAllowed));
				}
				for (int k = 0; k < cubeGrant.hierarchyGrants.length; k++) {
					MondrianDef.HierarchyGrant hierarchyGrant = cubeGrant.hierarchyGrants[k];
					Hierarchy hierarchy = (Hierarchy) Util.lookupCompound(
							schemaReader, cube, Util.explode(hierarchyGrant.hierarchy), true, Category.Hierarchy);
					final int hierarchyAccess = getAccess(hierarchyGrant.access, hierarchyAllowed);
					Level topLevel = null;
					if (hierarchyGrant.topLevel != null) {
						if (hierarchyAccess != Access.CUSTOM) {
							throw Util.newError("You may only specify 'topLevel' if access='custom'");
						}
						topLevel = (Level) Util.lookupCompound(schemaReader, cube, Util.explode(hierarchyGrant.topLevel), true, Category.Level);
					}
					Level bottomLevel = null;
					if (hierarchyGrant.bottomLevel != null) {
						if (hierarchyAccess != Access.CUSTOM) {
							throw Util.newError("You may only specify 'bottomLevel' if access='custom'");
						}
						bottomLevel = (Level) Util.lookupCompound(schemaReader, cube, Util.explode(hierarchyGrant.bottomLevel), true, Category.Level);
					}
					role.grant(hierarchy, hierarchyAccess, topLevel, bottomLevel);
					for (int m = 0; m < hierarchyGrant.memberGrants.length; m++) {
						if (hierarchyAccess != Access.CUSTOM) {
							throw Util.newError("You may only specify <MemberGrant> if <Hierarchy> has access='custom'");
						}
						MondrianDef.MemberGrant memberGrant = hierarchyGrant.memberGrants[m];
						Member member = schemaReader.getMemberByUniqueName(Util.explode(memberGrant.member),true);
						if (member.getHierarchy() != hierarchy) {
							throw Util.newError("Member '" + member + "' is not in hierarchy '" + hierarchy + "'");
						}
						role.grant(member, getAccess(memberGrant.access, memberAllowed));
					}
				}
			}
		}
		role.makeImmutable();
		return role;
	}

	private int getAccess(String accessString, int[] allowed) {
		final int access = Access.instance().getOrdinal(accessString);
		for (int i = 0; i < allowed.length; i++) {
			if (access == allowed[i]) {
				return access; // value is ok
			}
		}
		throw Util.newError("Bad value access='" + accessString + "'");
	}

	public Dimension createDimension(Cube cube, String xml) {
		MondrianDef.CubeDimension xmlDimension = null;
		try {
			final Parser xmlParser = XOMUtil.createDefaultParser();
			final DOMWrapper def = xmlParser.parse(xml);
			final String tagName = def.getTagName();
			if (tagName.equals("Dimension")) {
				xmlDimension = new MondrianDef.Dimension(def);
			} else if (tagName.equals("DimensionUsage")) {
				xmlDimension = new MondrianDef.DimensionUsage(def);
			} else {
				throw new XOMException("Got <" + tagName +
						"> when expecting <Dimension> or <DimensionUsage>");
			}
		} catch (XOMException e) {
			throw Util.newError(e, "Error while adding dimension to cube '" +
					cube + "' from XML [" + xml + "]");
		}
		return ((RolapCube) cube).createDimension(xmlDimension);
	}

	/**
	 * A pool is a collection of schemas. Call <code>Pool.instance.{@link #get
	 * get(catalogName,jdbcConnectString)</code>.
	 */
	static class Pool {
		private static Pool pool = new Pool();

		private HashMap mapUrlToSchema = new HashMap();

		private Pool() {
		}

		static Pool instance() {
			return pool;
		}

		synchronized RolapSchema get(
				String catalogName, String jdbcConnectString,
				String jdbcUser, String dataSource, Util.PropertyList connectInfo) {
			final String key = makeKey(catalogName, jdbcConnectString, jdbcUser, dataSource);
			RolapSchema schema = (RolapSchema) mapUrlToSchema.get(key);
			if (schema == null) {
				schema = new RolapSchema(catalogName, connectInfo);
				mapUrlToSchema.put(key, schema);
				// Must create RolapConnection after we add to map, otherwise
				// we will loop.
				// no, this is redundant - its set in the ctor of RolapSchema
				// schema.internalConnection = new RolapConnection(connectInfo, schema);
			}
			return schema;
		}

		synchronized void remove(String catalogName, String jdbcConnectString, String jdbcUser, String dataSource) {
			mapUrlToSchema.remove(makeKey(catalogName, jdbcConnectString, jdbcUser, dataSource));
		}

		/**
		 * Creates a key with which to identify a schema in the cache.
		 */
		private static String makeKey(
				String catalogName, String jdbcConnectString, String jdbcUser, String dataSource) {
			StringBuffer buf = new StringBuffer();
			appendIfNotNull(buf, catalogName);
			appendIfNotNull(buf, jdbcConnectString);
			appendIfNotNull(buf, jdbcUser);
			appendIfNotNull(buf, dataSource);
			String key = buf.toString();
			return key;
		}

		private static void appendIfNotNull(StringBuffer buf, String s) {
			if (s != null) {
				if (buf.length() > 0) {
					buf.append('.');
				}
				buf.append(s);
			}
		}
	}

	public static void flushSchema(String catalogName, String jdbcConnectString, String jdbcUser, String dataSource) {
		Pool.instance().remove(catalogName, jdbcConnectString, jdbcUser, dataSource);
	}

	public Cube lookupCube(String cube,boolean failIfNotFound)
	{
		Cube mdxCube = lookupCube(cube);
		if (mdxCube == null && failIfNotFound)
			throw Util.getRes().newMdxCubeNotFound(cube);
		return mdxCube;
	}

	/**
	 * Finds a cube called 'cube' in the current catalog, or return null if no
	 * cube exists.
	 */
	protected Cube lookupCube(String cube) {
		return (RolapCube) mapNameToCube.get(cube);
	}

	public Cube[] getCubes() {
		return (RolapCube[]) mapNameToCube.values().toArray(new RolapCube[0]);
	}

	public Hierarchy[] getSharedHierarchies() {
		return (RolapHierarchy[])
				mapSharedHierarchyNameToHierarchy.values().toArray(
						new RolapHierarchy[0]);
	}

	RolapHierarchy getSharedHierarchy(String name) {
		return (RolapHierarchy) mapSharedHierarchyNameToHierarchy.get(name);
	}

	public Role lookupRole(String role) {
		return (Role) mapNameToRole.get(role);
	}

	/**
	 * Gets a {@link MemberReader} with which to read a hierarchy. If the
	 * hierarchy is shared (<code>sharedName</code> is not null), looks up
	 * a reader from a cache, or creates one if necessary.
	 *
	 * @synchronization thread safe
	 */
	synchronized MemberReader createMemberReader(
			String sharedName, RolapHierarchy hierarchy, MondrianDef.Hierarchy xmlHierarchy) {
		MemberReader reader;
		if (sharedName != null) {
			reader = (MemberReader) mapSharedHierarchyToReader.get(sharedName);
			if (reader == null) {
				reader = createMemberReader(hierarchy, xmlHierarchy);
				// share, for other uses of the same shared hierarchy
				mapSharedHierarchyToReader.put(sharedName, reader);
				mapSharedHierarchyNameToHierarchy.put(sharedName, hierarchy);
			} else {
				final RolapHierarchy sharedHierarchy = (RolapHierarchy)
						mapSharedHierarchyNameToHierarchy.get(sharedName);
				final RolapDimension sharedDimension = (RolapDimension)
						sharedHierarchy.getDimension();
				final RolapDimension dimension = (RolapDimension) hierarchy.getDimension();
				Util.assertTrue(
						dimension.getGlobalOrdinal() ==
						sharedDimension.getGlobalOrdinal());
			}
		} else {
			reader = createMemberReader(hierarchy, xmlHierarchy);
		}
		return reader;
	}

	/**
	 * Creates a {@link MemberReader} with which to read a hierarchy.
	 */
	private MemberReader createMemberReader(
			RolapHierarchy hierarchy, MondrianDef.Hierarchy xmlHierarchy) {
		if (xmlHierarchy.memberReaderClass != null) {
			Exception e2 = null;
			try {
				Properties properties = null;
				Class clazz = Class.forName(
					xmlHierarchy.memberReaderClass);
				Constructor constructor = clazz.getConstructor(new Class[] {
					RolapHierarchy.class, Properties.class});
				Object o = constructor.newInstance(
					new Object[] {hierarchy, properties});
				if (o instanceof MemberReader) {
					return (MemberReader) o;
				} else if (o instanceof MemberSource) {
					return new CacheMemberReader((MemberSource) o);
				} else {
					throw Util.getRes().newInternal(
						"member reader class " + clazz +
						" does not implement " + MemberSource.class);
				}
			} catch (ClassNotFoundException e) {
				e2 = e;
			} catch (NoSuchMethodException e) {
				e2 = e;
			} catch (InstantiationException e) {
				e2 = e;
			} catch (IllegalAccessException e) {
				e2 = e;
			} catch (InvocationTargetException e) {
				e2 = e;
			}
			throw Util.getRes().newInternal(
					"while instantiating member reader '" +
					xmlHierarchy.memberReaderClass,
					e2);
		} else {
			SqlMemberSource source = new SqlMemberSource(hierarchy);
			int memberCount = source.getMemberCount();
			int largeDimensionThreshold =
					MondrianProperties.instance().getLargeDimensionThreshold();
			if (memberCount > largeDimensionThreshold) {
				return new SmartMemberReader(source);
			} else {
				return new CacheMemberReader(source);
			}
		}
	}

	synchronized HierarchyUsage getUsage(
			RolapHierarchy hierarchy, RolapCube cube) {
		HierarchyUsage usageKey = hierarchy.createUsage(cube.getFact()),
			usage = (HierarchyUsage) hierarchyUsages.get(usageKey);
		if (usage == null) {
			hierarchyUsages.put(usageKey, usageKey);
			return usageKey;
		} else {
			return usage;
		}
	}

	public SchemaReader getSchemaReader() {
		return new RolapSchemaReader(defaultRole) {
			public Cube getCube() {
				throw new UnsupportedOperationException();
			}
		};
	}

	/**
	 * Connection for purposes of parsing and validation. Careful! It won't
	 * have the correct locale or access-control profile.
	 */
	RolapConnection getInternalConnection() {
		return internalConnection;
	}

	private Role createDefaultRole() {
		Role role = new Role();
		role.grant(this, Access.ALL);
		role.makeImmutable();
		return role;
	}

	/**
	 * An <code>Extender</code> is a user-supplied class which extends the
	 * system in some way. The <code>Extender</code> interfaces allows it to
	 * describe itself, and receive parameters.
	 **/
	interface Extender
	{
		String getDescription();
		ExtenderParameter[] getParameterDescriptions();
		void initialize(Properties properties);
	}

	/**
	 * An <code>ExtenderParameter</code> describes a parameter of an {@link
	 * Extender}.
	 **/
	static class ExtenderParameter
	{
		String name;
		Class clazz;
		String description;
	}

}

// End RolapSchema.java
