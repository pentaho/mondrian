/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 July, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;

import java.sql.SQLException;
import java.util.*;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;

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
	/**
	 * Internal use only.
	 */
	private RolapConnection internalConnection;
	/**
	 * Holds cubes in this schema.
	 */
	final HashMap mapNameToCube = new HashMap();
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
	final HashMap mapSharedHierarchyNameToHierarchy = new HashMap();

	/**
	 * Creates a {@link RolapSchema}. Use
	 * <code>RolapSchema.Pool.instance().get(catalogName)</code>.
	 */
	private RolapSchema(String catalogName, Util.PropertyList connectInfo) {
		internalConnection = new RolapConnection(connectInfo, this);
		try {
			java.net.URL url = new java.net.URL(catalogName);
			mondrian.xom.Parser xmlParser =
				mondrian.xom.XOMUtil.createDefaultParser();
			MondrianDef.Schema xmlSchema = new MondrianDef.Schema(
				xmlParser.parse(url));
			load(xmlSchema);
		} catch (mondrian.xom.XOMException e) {
			throw Util.newError(e, "while parsing catalog " + catalogName);
		} catch (java.io.IOException e) {
			throw Util.newError(e, "while parsing catalog " + catalogName);
		}
	}

	private void load(MondrianDef.Schema xmlSchema)
	{
		for (int i = 0; i < xmlSchema.cubes.length; i++) {
			MondrianDef.Cube xmlCube = xmlSchema.cubes[i];
			RolapCube cube = new RolapCube(this, xmlSchema, xmlCube);
			if (false && xmlCube.name.equals("Sales")) {
				cube = new RolapCube(this);
			}
			mapNameToCube.put(xmlCube.name, cube);
		}
		for (int i = 0; i < xmlSchema.virtualCubes.length; i++) {
			MondrianDef.VirtualCube xmlVirtualCube = xmlSchema.virtualCubes[i];
			RolapCube cube = new RolapCube(this, xmlSchema, xmlVirtualCube);
			mapNameToCube.put(xmlVirtualCube.name, cube);
		}
	}

	private static Pool pool = new Pool();
	/**
	 * A pool is a collection of schemas. Call <code>Pool.instance.{@link #get
	 * get(catalogName,jdbcConnectString)</code>.
	 */
	static class Pool {
		HashMap mapUrlToSchema = new HashMap();
		static Pool instance() {
			return pool;
		}
		synchronized RolapSchema get(
				String catalogName, String jdbcConnectString,
				Util.PropertyList connectInfo) {
			final String key = catalogName + ":" + jdbcConnectString;
			RolapSchema schema = (RolapSchema) mapUrlToSchema.get(key);
			if (schema == null) {
				schema = new RolapSchema(catalogName, connectInfo);
				mapUrlToSchema.put(key, schema);
				// Must create RolapConnection after we add to map, otherwise
				// we will loop.
				schema.internalConnection = new RolapConnection(connectInfo, schema);
			}
			return schema;
		}
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

	public String[] listCubeNames()
	{
		throw new UnsupportedOperationException();
	}

	RolapHierarchy getSharedHierarchy(String name) {
		return (RolapHierarchy) mapSharedHierarchyNameToHierarchy.get(name);
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
				e2, "while instantiating member reader '" +
				xmlHierarchy.memberReaderClass);
		} else {
			SqlMemberSource source = new SqlMemberSource(hierarchy);
			int memberCount = source.getMemberCount();
			if (memberCount > RolapHierarchy.LARGE_DIMENSION_THRESHOLD &&
				source.canDoRolap()) {
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

	/**
	 * Connection for purposes of parsing and validation. Careful! It won't
	 * have the correct locale or access-control profile.
	 */
	RolapConnection getInternalConnection() {
		return internalConnection;
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
