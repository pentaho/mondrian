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
	/** Name of pk column in dimension table. **/
	String primaryKey;
	/** Name of dimension table which contains the primary key for the
	 * hierarchy. (Usually the table of the lowest level of the hierarchy.) */
	String primaryKeyTable;

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
		setKeys(null, primaryKey, foreignKey);
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
		if ((xmlHierarchy.relation == null ? 0 : 1) +
				(xmlHierarchy.memberReaderClass == null ? 0 : 1) != 1) {
			throw Util.newError(
					"Hierarchy '" + getUniqueName() +
					"' must have either a member reader class or " +
					"a source relation (<Table>, <Join> or <View>)");
		}
		setKeys(
				xmlHierarchy.primaryKeyTable, xmlHierarchy.primaryKey,
				xmlCubeDimension.foreignKey);
	}

	private void setKeys(
			String primaryKeyTable, String primaryKey, String foreignKey) {
		if (this.getDimension().isMeasures() ||
				this.xmlHierarchy.memberReaderClass != null) {
			Util.assertTrue(primaryKeyTable == null);
			Util.assertTrue(primaryKey == null);
			Util.assertTrue(foreignKey == null);
			return;
		}
		this.primaryKey = primaryKey;
		if (primaryKey == null) {
			throw Util.newError(
					"must specify primaryKey for hierarchy " + getUniqueName());
		}
		if (primaryKeyTable == null) {
			primaryKeyTable = getUniqueTableName();
			if (primaryKeyTable == null) {
				throw Util.newError(
						"must specify primaryKeyTable for hierarchy " +
						getUniqueName() +
						", because it has more than one table");
			}
		}
		this.primaryKeyTable = primaryKeyTable;
		RolapCube cube = (RolapCube) getCube();
		if (!cube.isVirtual()) {
			// virtual cubes don't create usages
			HierarchyUsage usage = getUsage(cube);
			if (foreignKey == null) {
				throw Util.newError(
						"must specify foreignKey for hierarchy " + getUniqueName());
			}
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

	RolapLevel newLevel(
			String name, MondrianDef.Expression nameExp,
			MondrianDef.Expression ordinalExp, int flags) {
		RolapLevel level = new RolapLevel(
				this, this.levels.length, name, nameExp, ordinalExp,
				flags);
		this.levels = (RolapLevel[]) RolapUtil.addElement(this.levels, level);
		return level;
	}

	RolapLevel newLevel(
			String name, String table, String column, String ordinalColumn, int flags) {
		return newLevel(
				name,
				new MondrianDef.Column(table, column),
				new MondrianDef.Column(table, ordinalColumn),
				flags);
	}

	RolapLevel newLevel(String name, String table, String column, String ordinalColumn) {
		return newLevel(
				name,
				new MondrianDef.Column(table, column),
				new MondrianDef.Column(table, ordinalColumn),
				0);
	}

	RolapLevel newLevel(String name, String table, String column) {
		return newLevel(name, new MondrianDef.Column(table, column), null, 0);
	}

	RolapLevel newLevel(String name) {
		return newLevel(name, null, null, 0);
	}

	/**
	 * If this hierarchy has precisely one table, returns that table's alias,
	 * otherwise, returns null.
	 */
	String getUniqueTableName() {
		if (xmlHierarchy != null) {
			MondrianDef.Relation relation = xmlHierarchy.relation;
			if (relation instanceof MondrianDef.Table) {
				return ((MondrianDef.Table) relation).getAlias();
			} else if (relation instanceof MondrianDef.View) {
				return ((MondrianDef.View) relation).alias;
			}
		}
		return null;
	}

	boolean tableExists(String tableName) {
		return xmlHierarchy != null &&
				xmlHierarchy.relation != null &&
				tableExists(tableName, xmlHierarchy.relation);
	}

	private static boolean tableExists(
			String tableName, MondrianDef.Relation relation) {
		if (relation instanceof MondrianDef.Table) {
			return ((MondrianDef.Table) relation).name.equals(tableName);
		}
		if (relation instanceof MondrianDef.Join) {
			MondrianDef.Join join = (MondrianDef.Join) relation;
			return tableExists(tableName, join.left) ||
				tableExists(tableName, join.right);
		}
		return false;
	}
	private MemberReader createMemberReader()
	{
		if (this.memberReader != null) {
			return this.memberReader;
		}
		if (xmlHierarchy.relation != null) {
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
			"cannot create member reader for hierarchy '" + getUniqueName() + "'");
	}

    MondrianDef.Relation getRelation() {
		if (xmlHierarchy == null) {
			return null;
		} else {
			return xmlHierarchy.relation;
		}
	}

	public Member getDefaultMember()
	{
		// use lazy initialization to get around bootstrap issues
		if (defaultMember == null) {
			RolapMember[] rootMembers = memberReader.getRootMembers();
			if (rootMembers.length == 0) {
				throw Util.newError(
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
	/**
	 * Adds to the FROM clause of the query the tables necessary to access the
	 * members of this hierarchy. If <code>level</code> is not null, adds only
	 * the tables necessary to access that level (which may be fewer tables).
	 *
	 * <p> This method is idempotent: if you call it more than once, it onle
	 * adds the table(s) to the FROM clause once.
	 *
	 * @param query Query to add the hierarchy to
	 * @param level Level to qualify up to; if null, qualifies up to the
	 *    topmost ('all') level, which may require more columns and more joins
	 * @param cube If not null, include the cube's fact table in the query and
	 *    join to it. Member readers generally do not want to join the cube,
	 *    but measure readers do.
	 */
	void addToFrom(SqlQuery query, RolapLevel level, RolapCube cube)
	{
		if (xmlHierarchy != null) {
			final boolean failIfExists = false;
			if (cube != null) {
				cube.addToFrom(query);
				MondrianDef.Relation firstTable = xmlHierarchy.relation.find(
						primaryKeyTable);
				query.addFrom(firstTable, failIfExists);
				HierarchyUsage hierarchyUsage = getUsage(cube);
				query.addWhere(
						query.quoteIdentifier(
								cube.getAlias(), hierarchyUsage.foreignKey) +
						" = " +
						query.quoteIdentifier(
								primaryKeyTable, primaryKey));
			}
			MondrianDef.Relation relation = xmlHierarchy.relation;
			if (relation instanceof MondrianDef.Join) {
				MondrianDef.Join join = (MondrianDef.Join) relation;
				if (level != null) {
					// Suppose relation is
					//   (((A join B) join C) join D)
					// and the fact table is
					//   F
					// and our level uses C. We want to make the expression
					//   F left join ((A join B) join C).
					// Search for the smallest subset of the relation which
					// uses C.
					relation = relationSubset(
							relation, level.nameExp.getTableAlias());
				}
			}
			query.addFrom(relation, failIfExists);
		} else {
			throw Util.newInternal(
					"cannot add hierarchy " + getUniqueName() +
					" to query: it does not have a <Table>, <View> or <Join>");
		}
	}

	/**
	 * Returns the smallest subset of <code>relation</code> which contains
	 * the relation <code>alias</code>, or null if these is no relation with
	 * such an alias.
	 */
	private static MondrianDef.Relation relationSubset(
			MondrianDef.Relation relation, String alias) {
		if (relation instanceof MondrianDef.Table) {
			MondrianDef.Table table = (MondrianDef.Table) relation;
			if (table.getAlias().equals(alias)) {
				return relation;
			} else {
				return null;
			}
		} else if (relation instanceof MondrianDef.Join) {
			MondrianDef.Join join = (MondrianDef.Join) relation;
			MondrianDef.Relation rightRelation = relationSubset(join.right, alias);
			if (rightRelation == null) {
				return relationSubset(join.left, alias);
			} else {
				return join;
			}
		} else {
			throw Util.newInternal("bad relation type " + relation);
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
	HierarchyUsage getUsage(RolapCube cube)
	{
		String factSchema = cube.factSchema;
		String factTable = cube.factTable;
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
