/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.rolap.sql.SqlQuery;

import java.util.List;

/**
 * <code>RolapHierarchy</code> implements {@link Hierarchy} for a ROLAP database.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapHierarchy extends HierarchyBase
{
	/** The raw member reader. For a member reader which incorporates access
	 * control, call {@link #getMemberReader(Role)}. */
	MemberReader memberReader;
	MondrianDef.Hierarchy xmlHierarchy;
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

	String foreignKey;
	private FunCall aggregateChildrenExpression;

	RolapHierarchy(RolapDimension dimension, String subName, boolean hasAll)
	{
		this.dimension = dimension;
		this.subName = subName;
		this.hasAll = hasAll;
		this.levels = new RolapLevel[0];
		this.name = dimension.getName();
		this.uniqueName = dimension.getUniqueName();
		if (this.subName != null) {
			this.name += "." + subName; // e.g. "Time.Weekly"
			this.uniqueName = Util.makeFqName(name); // e.g. "[Time.Weekly]"
		}
		if (hasAll) {
			Util.discard(newLevel("(All)", RolapLevel.ALL | RolapLevel.UNIQUE));
			this.allMemberName = "All " + name + "s";
		}
	}

	/**
	 * @param cube optional
	 */
	RolapHierarchy(
			RolapCube cube, RolapDimension dimension,
			MondrianDef.Hierarchy xmlHierarchy,
			MondrianDef.CubeDimension xmlCubeDimension)
	{
		this(dimension, xmlHierarchy.name, xmlHierarchy.hasAll.booleanValue());
		if (xmlHierarchy.relation == null &&
				xmlHierarchy.memberReaderClass == null &&
				cube != null) {
			xmlHierarchy.relation = cube.fact;
		}
		this.xmlHierarchy = xmlHierarchy;
		if (hasAll) {
			if (xmlHierarchy.allMemberName != null) {
				this.allMemberName = xmlHierarchy.allMemberName;
			}
			this.levels = new RolapLevel[xmlHierarchy.levels.length + 1];
			this.levels[0] = new RolapLevel(
					this, 0, "(All)", null, null, null, null, new RolapProperty[0], RolapLevel.ALL | RolapLevel.UNIQUE);
			for (int i = 0; i < xmlHierarchy.levels.length; i++) {
				final MondrianDef.Level xmlLevel = xmlHierarchy.levels[i];
				if (xmlLevel.getKeyExp() == null &&
						xmlHierarchy.memberReaderClass == null) {
					throw Util.newError("Level '" + xmlLevel.name + "' must have a name expression (a 'column' attribute or an <Expression> child)");
				}
				levels[i + 1] = new RolapLevel(this, i + 1, xmlLevel);
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
		if (xmlHierarchy.relation != null &&
				xmlHierarchy.memberReaderClass != null) {
			throw Util.newError(
					"Hierarchy '" + getUniqueName() +
					"' must not have more than one source " +
					" (memberReaderClass, <Table>, <Join> or <View>)");
		}
		this.foreignKey = xmlCubeDimension.foreignKey;
	}

	/**
	 * Initializes a hierarchy within the context of a cube.
	 */
	void init(RolapCube cube)
	{
		for (int i = 0; i < levels.length; i++) {
			((RolapLevel) levels[i]).init();
		}
		if (this.memberReader == null) {
			this.memberReader = getSchema().createMemberReader(
					sharedHierarchy, this, xmlHierarchy);
		}
		if (this.getDimension().isMeasures() ||
				this.xmlHierarchy.memberReaderClass != null) {
			Util.assertTrue(foreignKey == null);
			return;
		}
		if (!cube.isVirtual()) {
			// virtual cubes don't create usages
			HierarchyUsage usage = getSchema().getUsage(this,cube);
			usage.init(cube, this, null);
		}
	}

	RolapLevel newLevel(String name, int flags) {
		RolapLevel level = new RolapLevel(
				this, this.levels.length, name, null, null,
				null, null, new RolapProperty[0], flags);
		this.levels = (RolapLevel[]) RolapUtil.addElement(this.levels, level);
		return level;
	}

	/**
	 * If this hierarchy has precisely one table, returns that table;
	 * if this hierarchy has no table, return the cube's fact-table;
	 * otherwise, returns null.
	 */
	MondrianDef.Relation getUniqueTable() {
		Util.assertTrue(xmlHierarchy != null);
		MondrianDef.Relation relation = xmlHierarchy.relation;
		if (relation instanceof MondrianDef.Table ||
				relation instanceof MondrianDef.View) {
			return relation;
		} else if (relation instanceof MondrianDef.Join) {
			return null;
		} else {
			throw Util.newInternal(
					"hierarchy's relation is a " + relation.getClass());
		}
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

	RolapSchema getSchema() {
		return ((RolapDimension) dimension).schema;
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
			List rootMembers = memberReader.getRootMembers();
			if (rootMembers.size() == 0) {
				throw Util.newError(
					"cannot get default member: hierarchy " + getUniqueName() +
					" has no root members");
			}
			defaultMember = (RolapMember) rootMembers.get(0);
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

	String getAlias() {
		return getName();
	}

	/**
	 * Adds to the FROM clause of the query the tables necessary to access the
	 * members of this hierarchy. If <code>expression</code> is not null, adds
	 * the tables necessary to compute that expression.
	 *
	 * <p> This method is idempotent: if you call it more than once, it only
	 * adds the table(s) to the FROM clause once.
	 *
	 * @param query Query to add the hierarchy to
	 * @param expression Level to qualify up to; if null, qualifies up to the
	 *    topmost ('all') expression, which may require more columns and more joins
	 * @param cube If not null, include the cube's fact table in the query and
	 *    join to it. Member readers generally do not want to join the cube,
	 *    but measure readers do.
	 */
	void addToFrom(
			SqlQuery query, MondrianDef.Expression expression, RolapCube cube) {
		if (xmlHierarchy == null) {
			throw Util.newError(
					"cannot add hierarchy " + getUniqueName() +
					" to query: it does not have a <Table>, <View> or <Join>");
		}
		final boolean failIfExists = false;
		if (cube != null) {
			HierarchyUsage hierarchyUsage = getSchema().getUsage(this,cube);
			query.addFrom(cube.getFact(), failIfExists);
			query.addFrom(hierarchyUsage.joinTable, failIfExists);
			query.addWhere(
					query.quoteIdentifier(
							cube.getAlias(), hierarchyUsage.foreignKey) +
					" = " +
					hierarchyUsage.joinExp.getExpression(query));
		}
		MondrianDef.Relation relation = xmlHierarchy.relation;
		if (relation instanceof MondrianDef.Join) {
			if (expression != null) {
				// Suppose relation is
				//   (((A join B) join C) join D)
				// and the fact table is
				//   F
				// and our expression uses C. We want to make the expression
				//   F left join ((A join B) join C).
				// Search for the smallest subset of the relation which
				// uses C.
				relation = relationSubset(relation, expression.getTableAlias());
			}
		}
		query.addFrom(relation, failIfExists);
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

	HierarchyUsage createUsage(MondrianDef.Relation fact) {
		if (sharedHierarchy == null) {
			return new PrivateHierarchyUsage(fact, this);
		} else {
			return new SharedHierarchyUsage(fact, sharedHierarchy);
		}
	}

	/**
	 * Returns a member reader which enforces the access-control profile of
	 * <code>role</code>.
	 *
	 * @pre role != null
	 * @post return != null
	 */
	MemberReader getMemberReader(Role role) {
		final int access = role.getAccess(this);
		switch (access) {
		case Access.NONE:
			throw Util.newInternal("Illegal access to members of hierarchy " + this);
		case Access.ALL:
			return memberReader;
		case Access.CUSTOM:
			return new RestrictedMemberReader(memberReader, role);
		default:
			throw Util.newInternal("Bad case " + access);
		}
	}

	/**
	 * Returns an expression which will compute a member's value by aggregating
	 * its children.
	 *
	 * <p>It is efficient to share one expression between all calculated members in
	 * a parent-child hierarchy, so we only need need to validate the expression
	 * once.
	 */
	synchronized Exp getAggregateChildrenExpression() {
		if (aggregateChildrenExpression == null) {
			aggregateChildrenExpression = new FunCall("$AggregateChildren",
					new Exp[] {this}, FunDef.TypeInternal);
		}
		return aggregateChildrenExpression;
	}
}

/**
 * A <code>RolapNullMember</code> is the null member of its hierarchy.
 * Every hierarchy has precisely one. They are yielded by operations such as
 * <code>[Gender].[All].ParentMember</code>. Null members are usually omitted
 * from sets (in particular, in the set constructor operator "{ ... }".
 */
class RolapNullMember extends RolapMember {
	RolapNullMember(RolapHierarchy hierarchy) {
		super(
			null, (RolapLevel) hierarchy.getLevels()[0], null, "#Null");
		this.memberType = NULL_MEMBER_TYPE;
	}
}

// End RolapHierarchy.java
