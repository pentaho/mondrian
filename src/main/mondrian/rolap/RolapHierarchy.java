/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.rolap.sql.SqlQuery;

import java.util.Properties;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * <code>RolapHierarchy</code> implements {@link Hierarchy} for a ROLAP database.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapHierarchy extends HierarchyBase
{
	MemberReader memberReader;
	private MondrianDef.Hierarchy xmlHierarchy;
	private RolapMember defaultMember;
	private RolapNullMember nullMember;
	/**
	 * If this hierarchy is a public -- that is, it belongs to a dimension
	 * which is a usage of a shared dimension -- then
	 * <code>sharedHierarchy</code> holds the unique name of the shared
	 * hierarchy; otherwise it is null.
	 *
	 * <p> Suppose this hierarchy is "Weekly" in the dimension "Order Date" of
	 * cube "Sales", and that "Order Date" is a usage of the "Time"
	 * dimension. Then <code>sharedHierarchy</code> will be "[Time].[Weekly]".
	 **/
	private String sharedHierarchy;
	/**
	 * Source query, for example "select * from product join *
	 * product_category". Level- and measure-column names refer to
	 * expressions in this select list.
	 **/
//	String sql;
	String primaryKey; // name of pk column in dimension table
//	String foreignKey; // name of fk column in fact table

	/**
	 * If a dimension has more than this number of members, use a {@link
	 * SmartMemberReader}.
	 **/
	static final int LARGE_DIMENSION_THRESHOLD = 100;

//  	/** @deprecated **/
//  	RolapHierarchy(String subName, boolean hasAll, RolapLevel[] levels)
//  	{
//  		this.subName = subName;
//  		this.hasAll = hasAll;
//  		if (hasAll) {
//  			this.levels = new RolapLevel[levels.length + 1];
//  			this.levels[0] = new RolapLevel(this, "(All)", null, null);
//  			System.arraycopy(levels, 0, this.levels, 1, levels.length);
//  		} else {
//  			this.levels = levels;
//  		}
//  	}

	private RolapHierarchy(RolapDimension dimension, String subName, boolean hasAll)
	{
		this.dimension = dimension;
		this.subName = subName;
		this.hasAll = hasAll;
		this.levels = new RolapLevel[0];
		if (hasAll) {
			Util.discard(newLevel("(All)", null, null, RolapLevel.ALL));
		}
		this.name = dimension.getName();
		this.uniqueName = dimension.getUniqueName();
		if (this.subName != null) {
			this.name += "." + subName; // e.g. "Time.Weekly"
			this.uniqueName = Util.makeFqName(name); // e.g. "[Time.Weekly]"
		}
	}

	RolapHierarchy(
		RolapDimension dimension, String subName, boolean hasAll,
		String sql, String primaryKey, String foreignKey)
	{
		this(dimension, subName, hasAll);
		Util.assertTrue(sql == null);
		this.primaryKey = primaryKey;
		setForeignKey(foreignKey);
	}

	RolapHierarchy(
		RolapDimension dimension,
		MondrianDef.Hierarchy xmlHierarchy,
		MondrianDef.CubeDimension xmlCubeDimension)
	{
		this(dimension, xmlHierarchy.name, xmlHierarchy.hasAll.booleanValue());
		this.xmlHierarchy = xmlHierarchy;
		if (hasAll) {
			this.levels = new RolapLevel[xmlHierarchy.levels.length + 1];
			this.levels[0] = new RolapLevel(
				this, 0, "(All)", null, null, RolapLevel.ALL);
			for (int i = 0; i < xmlHierarchy.levels.length; i++) {
				levels[i + 1] = new RolapLevel(
					this, i + 1, xmlHierarchy.levels[i]);
			}
		} else {
			this.levels = new RolapLevel[xmlHierarchy.levels.length];
			for (int i = 0; i < xmlHierarchy.levels.length; i++) {
				levels[i] = new RolapLevel(this, i, xmlHierarchy.levels[i]);
			}
		}
		this.sharedHierarchy = null;
		if (xmlCubeDimension instanceof MondrianDef.DimensionUsage) {
			String sharedDimensionName =
				((MondrianDef.DimensionUsage) xmlCubeDimension).source;
			this.sharedHierarchy = sharedDimensionName;
			if (subName != null) {
				this.sharedHierarchy += "." + subName; // e.g. "Time.Weekly"
			}
		}
		if ((xmlHierarchy.table == null ? 0 : 1) +
			(xmlHierarchy.querySet == null ? 0 : 1) +
			(xmlHierarchy.memberReaderClass == null ? 0 : 1) != 1) {
			throw Util.newInternal(
					"Hierarchy '" + getUniqueName() +
					"' must have precisely one of the following: " +
					"a 'table' or 'memberReaderClass' attribute, " +
					"or an embedded '<Query>'");
		}
//  		if (xmlHierarchy.table != null) {
//  			this.sql = RolapUtil.doubleQuoteForSql(
//  				xmlHierarchy.schema, xmlHierarchy.table);
//  		} else if (xmlHierarchy.sql != null) {
//  			this.sql = "(" + xmlHierarchy.sql + ")";
//  		}
		this.primaryKey = xmlHierarchy.primaryKey;
		setForeignKey(xmlCubeDimension.foreignKey);
	}

	private void setForeignKey(String foreignKey)
	{
		RolapCube cube = (RolapCube) getCube();
		if (!cube.isVirtual()) {
			// virtual cubes don't create usages
			HierarchyUsage usage = getUsage(cube.factSchema, cube.factTable);
			usage.foreignKey = foreignKey;
		}
	}

	void init()
	{
		for (int i = 0; i < levels.length; i++) {
			((RolapLevel) levels[i]).init();
		}
		if (this.memberReader != null) {
		} else if (this.sharedHierarchy != null) {
			RolapConnection connection = (RolapConnection)
				getCube().getConnection();
			this.memberReader = (MemberReader)
				connection.mapSharedHierarchyToReader.get(
					this.sharedHierarchy);
			if (this.memberReader == null) {
				this.memberReader = createMemberReader();
				// share, for other uses of the same shared hierarchy
				connection.mapSharedHierarchyToReader.put(
					this.sharedHierarchy, this.memberReader);
			}
		} else {
			this.memberReader = createMemberReader();
		}
	}

	RolapLevel newLevel(String name, String column, String ordinalColumn, int flags)
	{
		RolapLevel level = new RolapLevel(
			this, this.levels.length, name, column, ordinalColumn, flags);
		this.levels = (RolapLevel[]) RolapUtil.addElement(
			this.levels, level);
		return level;
	}

	RolapLevel newLevel(String name, String column, String ordinalColumn)
	{
		return newLevel(name, column, ordinalColumn, 0);
	}

	RolapLevel newLevel(String name, String column)
	{
		return newLevel(name, column, null, 0);
	}

	private MemberReader createMemberReader()
	{
		if (this.memberReader != null) {
			return this.memberReader;
		}
		if (xmlHierarchy.querySet != null || xmlHierarchy.table != null) {
			SqlMemberSource source = new SqlMemberSource(this);
			int memberCount = source.getMemberCount();
			if (memberCount > LARGE_DIMENSION_THRESHOLD &&
				source.canDoRolap()) {
				return new SmartMemberReader(source);
			} else {
				return new CacheMemberReader(source);
			}
		}
		if (xmlHierarchy != null &&
			xmlHierarchy.memberReaderClass != null) {
			Exception e2 = null;
			try {
				Properties properties = null;
				Class clazz = Class.forName(
					xmlHierarchy.memberReaderClass);
				Constructor constructor = clazz.getConstructor(new Class[] {
					RolapHierarchy.class, Properties.class});
				Object o = constructor.newInstance(
					new Object[] {this, properties});
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
			if (e2 != null) {
				throw Util.getRes().newInternal(
					e2, "while instantiating member reader '" +
					xmlHierarchy.memberReaderClass);
			}
		}
		throw Util.newInternal(
			"cannot create member reader for hierarchy '" + name + "'");
	}

    MondrianDef.SQL[] getQuery() {
		if (xmlHierarchy == null ||
				xmlHierarchy.querySet == null) {
			return null;
		}
        return xmlHierarchy.querySet.selects;
    }

    String getTable() {
        if (xmlHierarchy == null) {
            return null;
        }
        return xmlHierarchy.table;
    }

    String getSchema() {
        if (xmlHierarchy == null) {
            return null;
        }
        return xmlHierarchy.schema;
    }

	public Member getDefaultMember()
	{
		// use lazy initialization to get around bootstrap issues
		if (defaultMember == null) {
			RolapMember[] rootMembers = memberReader.getRootMembers();
			if (rootMembers.length == 0) {
				throw Util.newInternal(
					"cannot get default member: hierarchy " + getUniqueName() +
					" has no root members");
			}
			defaultMember = rootMembers[0];
		}
		return defaultMember;
	}
	public Member getNullMember()
	{
		// use lazy initialization to get around bootstrap issues
		if (nullMember == null) {
			nullMember = new RolapNullMember(this);
		}
		return nullMember;
	}
	public Member[] getRootMembers()
	{
		return memberReader.getRootMembers();
	}
	public Member createMember(
		Member parent, Level level, String name, Formula formula)
	{
		if (formula != null) {
			return new RolapCalculatedMember(
				(RolapMember) parent, (RolapLevel) level, name, formula);
		} else {
			return new RolapMember(
				(RolapMember) parent, (RolapLevel) level, name);
		}
	}
	public int getMembersCount(int nMaxMembers)
	{
		return memberReader.getMemberCount();
	}
	String getAlias()
	{
		return getName();
	}
	void addToFrom(SqlQuery query)
	{
		if (xmlHierarchy.querySet != null) {
			String sqlString = query.chooseQuery(xmlHierarchy.querySet.selects);
			query.addFromQuery(sqlString, getAlias());
		} else if (xmlHierarchy.table != null) {
			query.addFromTable(
				xmlHierarchy.schema, xmlHierarchy.table, getAlias());
		} else {
			throw Util.newInternal(
				"cannot add hierarchy " + this + " to from clause: " +
				"'sql' and 'table' are both null");
		}
	}
	HierarchyUsage createUsage(String factSchema, String factTable)
	{
		if (sharedHierarchy == null) {
			return new PrivateHierarchyUsage(factSchema, factTable, this);
		} else {
			return new SharedHierarchyUsage(factSchema, factTable, sharedHierarchy);
		}
	}
	HierarchyUsage getUsage(String factSchema, String factTable)
	{
		RolapConnection connection = (RolapConnection)
			getCube().getConnection();
		HierarchyUsage usageKey = createUsage(factSchema, factTable),
			usage = (HierarchyUsage) connection.hierarchyUsages.get(usageKey);
		if (usage == null) {
			connection.hierarchyUsages.put(usageKey, usageKey);
			usage = usageKey;
		}
		return usage;
	}
}

/**
 * A <code>RolapNullMember</code> is the null member of its hierarchy.
 * Every hierarchy has precisely one. They are yielded by operations such as
 * <code>[Gender].[All].ParentMember</code>. Null members are usually omitted
 * from sets (see {@link mondrian.olap.fun.SetFunDef}).
 */
class RolapNullMember extends RolapMember {
	RolapNullMember(RolapHierarchy hierarchy) {
		super(
			null, (RolapLevel) hierarchy.getLevels()[0], null, "#Null");
		this.memberType = NULL_MEMBER_TYPE;
	}
}

// End RolapHierarchy.java
