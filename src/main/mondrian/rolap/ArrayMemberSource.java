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
import mondrian.olap.*;
import mondrian.rolap.sql.SqlQuery;

import java.util.Properties;
import java.util.Vector;
import java.sql.SQLException;

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
};

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
			Vector vector = new Vector();
			int ordinal = 0;
			RolapMember root = null;
			RolapLevel level = (RolapLevel) hierarchy.getLevels()[0];
			if (hierarchy.hasAll()) {
				root = new RolapMember(
					null, level, null, hierarchy.getAllMemberName());
				root.ordinal = ordinal++;
				vector.addElement(root);
				level = (RolapLevel) hierarchy.getLevels()[1];
			}
			for (int i = 0; i < values.length; i++) {
				RolapMember member = new RolapMember(root, level, values[i]);
				member.ordinal = ordinal++;
				vector.addElement(member);
			}
			RolapMember[] members = new RolapMember[vector.size()];
			vector.copyInto(members);
			return members;
		}
	};

	// implement MemberReader
	public void qualifyQuery(
		SqlQuery sqlQuery, RolapMember member)
	{
		if (member.isAll()) {
			// nothing
//  		} else if (false) {
//  			// Generate something like the following:
//  			// sales_fact_1997.customer_id in (
//  			//  select sales_fact_1997.customer_id
//  			//  from sales_fact_1997, product
//  			//  where product.product_id = sales_fact_1997.product_id
//  			//  and product.product_category = 'Dairy')
//  			RolapCube cube = (RolapCube) member.getCube();
//  			RolapHierarchy customerHierarchy = (RolapHierarchy)
//  				cube.lookupHierarchy("Customers", false);
//  			RolapMember dairyMember = (RolapMember)
//  				cube.lookupMemberCompound(
//  					Util.explode("[Product].[Food].[Dairy]"), true);
//  			RolapHierarchy productHierarchy = (RolapHierarchy)
//  				dairyMember.getHierarchy();
//  			boolean b = member.getName().equals("True");
//  			StringBuffer sb = new StringBuffer(
//  				RolapUtil.doubleQuoteForSql(
//  					customerHierarchy.getAlias(),
//  					customerHierarchy.foreignKey) +
//  				(b ? "" : " not") + " in (select " +
//  				RolapUtil.doubleQuoteForSql(
//  					cube.getAlias(), customerHierarchy.foreignKey) +
//  				" from (" + productHierarchy.sql + ") as " +
//  				RolapUtil.doubleQuoteForSql(productHierarchy.getAlias()) +
//  				", " +
//  				RolapUtil.doubleQuoteForSql(cube.factTable) +
//  				" as " +
//  				RolapUtil.doubleQuoteForSql(cube.getAlias()) +
//  				" where " +
//  				RolapUtil.doubleQuoteForSql(
//  					cube.getAlias(), productHierarchy.foreignKey) +
//  				" = " +
//  				RolapUtil.doubleQuoteForSql(
//  					productHierarchy.getAlias(),
//  					productHierarchy.primaryKey));
//  			for (RolapMember m = dairyMember; m != null; m = (RolapMember)
//  					 m.getParentMember()) {
//  				RolapLevel level = (RolapLevel) m.getLevel();
//  				if (level.column != null) {
//  					sb.append(
//  						" and " +
//  						RolapUtil.doubleQuoteForSql(
//  							productHierarchy.getAlias(), level.column) +
//  						" = " +
//  						m.quoteKeyForSql());
//  				}
//  			}
//  			sb.append(")");
//  			sqlQuery.addWhere(sb.toString());
		} else {
			// Generate something like the following:
			//
			// from sales_fact_1997 as "fact"
			// left join (
			//   select distinct sales_fact_1997.customer_id
			//   from sales_fact_1997, product
			//   where product.product_id = sales_fact_1997.product_id
			//   and product.product_category = 'Dairy') as "lookup"
			// on "lookup"."customer_id" = "fact"."customer_id"
			// ...
			// where "lookup"."customer_id" is null
			//
			RolapCube cube = (RolapCube) member.getCube();
			RolapHierarchy customerHierarchy = (RolapHierarchy)
				cube.lookupHierarchy("Customers", false);
			RolapMember dairyMember = (RolapMember)
				cube.lookupMemberCompound(
					Util.explode("[Product].[Food].[Dairy]"), true);
			RolapHierarchy productHierarchy = (RolapHierarchy)
				dairyMember.getHierarchy();
			boolean b = member.getName().equals("True");
			String productForeignKey = null, // productHierarchy.foreignKey
				customerForeignKey = null; // customerHierarchy.foreignKey
			StringBuffer sb = new StringBuffer(
				"(select distinct " +
				sqlQuery.quoteIdentifier(
					cube.getAlias(), customerForeignKey) +
				" from (" + productHierarchy.getQuery() + ") as " +
				sqlQuery.quoteIdentifier(productHierarchy.getAlias()) +
				", " +
				sqlQuery.quoteIdentifier(cube.factSchema, cube.factTable) +
				" as " +
				sqlQuery.quoteIdentifier(cube.getAlias()) +
				" where " +
				sqlQuery.quoteIdentifier(
					cube.getAlias(), productForeignKey) +
				" = " +
				sqlQuery.quoteIdentifier(
					productHierarchy.getAlias(),
					productHierarchy.primaryKey));
			for (RolapMember m = dairyMember; m != null; m = (RolapMember)
					 m.getParentMember()) {
				RolapLevel level = (RolapLevel) m.getLevel();
				if (level.column != null) {
					sb.append(
						" and " +
						sqlQuery.quoteIdentifier(
							productHierarchy.getAlias(), level.column) +
						" = " +
						m.quoteKeyForSql());
				}
			}
			sb.append(")");
			sqlQuery.addJoin(
				b ? "inner" : "left",
				sb.toString(),
				"lookup",
				sqlQuery.quoteIdentifier(
					"lookup", customerForeignKey) +
				" = " +
				sqlQuery.quoteIdentifier(
					cube.factTable, customerForeignKey));
			if (!b) {
				sqlQuery.addWhere(
					sqlQuery.quoteIdentifier(
						"lookup", productForeignKey) +
					" is null");
			}
		}
	}
};

// End ArrayMemberSource.java
