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
import mondrian.rolap.agg.AggregationManager;

import java.util.Hashtable;
import java.util.Vector;
import java.util.Properties;
import java.sql.*;

/**
 * <code>RolapCube</code> implements {@link Cube} for a ROLAP database.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapCube extends CubeBase
{
	RolapHierarchy measuresHierarchy;
	Hashtable mapNameToMember = new Hashtable();
	Hashtable mapElementToElement = new Hashtable();
	/** For SQL generator. Name of fact table. */
	String factTable;
    /** For SQL generator. Name of fact table's schema. May be null. */
	String factSchema;
	/** To access all measures stored in the fact table. */
	CellReader cellReader;

	RolapCube(
		RolapConnection connection, MondrianDef.Schema xmlSchema,
		MondrianDef.Cube xmlCube)
	{
		this.connection = connection;
		this.name = xmlCube.name;
		this.factTable = xmlCube.factTable;
		this.factSchema = xmlCube.factSchema;
		this.dimensions = new RolapDimension[xmlCube.dimensions.length + 1];
		RolapDimension measuresDimension = new RolapDimension(this, 0, "Measures");
		this.dimensions[0] = measuresDimension;
		this.measuresHierarchy = measuresDimension.newHierarchy(null, false, null, null, null);
		RolapLevel measuresLevel = this.measuresHierarchy.newLevel(
			"MeasuresLevel", null);
		for (int i = 0; i < xmlCube.dimensions.length; i++) {
			MondrianDef.CubeDimension xmlCubeDimension = xmlCube.dimensions[i];
			MondrianDef.Dimension xmlDimension = xmlCubeDimension.getDimension(xmlSchema);
			dimensions[i + 1] = new RolapDimension(this, i + 1, xmlDimension, xmlCubeDimension);
		}
		RolapMeasure measures[] = new RolapMeasure[
			xmlCube.measures.length];
		for (int i = 0; i < xmlCube.measures.length; i++) {
			measures[i] = RolapMeasure.create(
				null, measuresLevel, xmlCube.measures[i]);
		}
		this.measuresHierarchy.memberReader = new CacheMemberReader(
			new MeasureMemberSource(measuresHierarchy, measures));
		init();
	}

	/**
	 * Creates a <code>RolapCube</code> from a virtual cube.
	 **/
	RolapCube(
		RolapConnection connection, MondrianDef.Schema xmlSchema,
		MondrianDef.VirtualCube xmlVirtualCube)
	{
		this.connection = connection;
		this.name = xmlVirtualCube.name;
		this.factTable = null;
		this.factSchema = null;
		this.dimensions = new RolapDimension[
			xmlVirtualCube.dimensions.length + 1];
		RolapDimension measuresDimension = new RolapDimension(
			this, 0, "Measures");
		this.dimensions[0] = measuresDimension;
		this.measuresHierarchy = measuresDimension.newHierarchy(null, false, null, null, null);
		this.measuresHierarchy.newLevel("MeasuresLevel", null);
		for (int i = 0; i < xmlVirtualCube.dimensions.length; i++) {
			MondrianDef.VirtualCubeDimension xmlCubeDimension =
				xmlVirtualCube.dimensions[i];
			MondrianDef.Dimension xmlDimension = xmlCubeDimension.getDimension(
				xmlSchema);
			dimensions[i + 1] = new RolapDimension(
				this, i + 1, xmlDimension, xmlCubeDimension);
		}
		RolapMeasure measures[] = new RolapMeasure[
			xmlVirtualCube.measures.length];
		for (int i = 0; i < xmlVirtualCube.measures.length; i++) {
			// Lookup a measure in an existing cube. (Don't know whether it
			// will confuse things that this measure still points to its 'real'
			// cube.)
			MondrianDef.VirtualCubeMeasure xmlMeasure =
				xmlVirtualCube.measures[i];
			Cube cube = connection.lookupCube(xmlMeasure.cubeName);
			Member[] cubeMeasures = cube.getMeasures();
			for (int j = 0; j < cubeMeasures.length; j++) {
				if (cubeMeasures[j].getUniqueName().equals(xmlMeasure.name)) {
					measures[i] = (RolapMeasure) cubeMeasures[j];
					break;
				}
			}
			if (measures[i] == null) {
				throw Util.newInternal(
					"could not find measure '" + xmlMeasure.name +
					"' in cube '" + xmlMeasure.cubeName + "'");
			}
		}
		this.measuresHierarchy.memberReader = new CacheMemberReader(
			new MeasureMemberSource(measuresHierarchy, measures));
		init();
	}

	RolapCube(RolapConnection connection)
	{
		this.connection = connection;
		this.name = "Sales";
		this.factTable = "sales_fact_1997";
		this.dimensions = new RolapDimension[0];
		RolapDimension dimension;
		RolapHierarchy hierarchy;
		RolapLevel level;
		dimension = newDimension("Measures");
		hierarchy = dimension.newHierarchy(null, false, null, null, null);
		this.measuresHierarchy = hierarchy;
		level = hierarchy.newLevel("MeasuresLevel", null);
		RolapLevel measuresLevel = level;
		dimension = newDimension("Store");
		hierarchy = dimension.newHierarchy(
			null, true, "SELECT * FROM \"store\"", "store_id", "store_id");
		level = hierarchy.newLevel("Store Country", "store_country");
		level = hierarchy.newLevel("Store State", "store_state");
		level = hierarchy.newLevel("Store City", "store_city");
		level = hierarchy.newLevel("Store Name", "store_name");
		dimension = newDimension("Time");
		hierarchy = dimension.newHierarchy(
			null, false,
			"SELECT * FROM \"time_by_day\"", "time_id", "time_id");
		level = hierarchy.newLevel("Year", "the_year", null, RolapLevel.NUMERIC);
		level = hierarchy.newLevel("Quarter", "quarter");
		level = hierarchy.newLevel("Month", "month_of_year", null, RolapLevel.NUMERIC);
		dimension = newDimension("Product");
		hierarchy = dimension.newHierarchy(
			null, true,
			"SELECT * FROM \"product\", \"product_class\" " +
			"WHERE \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"",
			"product_id", "product_id");
		level = hierarchy.newLevel("Product Family", "product_family");
		level = hierarchy.newLevel("Product Department", "product_department");
		level = hierarchy.newLevel("Product Category", "product_category");
		level = hierarchy.newLevel("Product Subcategory", "product_subcategory");
		level = hierarchy.newLevel("Brand Name", "brand_name");
		level = hierarchy.newLevel("Product Name", "product_name");
		dimension = newDimension("Promotion Media");
		hierarchy = dimension.newHierarchy(null, true, "SELECT * FROM \"promotion\"", "promotion_id", "promotion_id");
		level = hierarchy.newLevel("Media Type", "media_type");
		dimension = newDimension("Promotions");
		hierarchy = dimension.newHierarchy(null, true, "SELECT * FROM \"promotion\"", "promotion_id", "promotion_id");
		level = hierarchy.newLevel("Promotion Name", "promotion_name");
		dimension = newDimension("Customers");
		hierarchy = dimension.newHierarchy(null, true, "SELECT *, \"fname\" + ' ' + \"lname\" as \"name\" FROM \"customer\"", "customer_id", "customer_id");
		level = hierarchy.newLevel("Country", "country");
		level = hierarchy.newLevel("State Province", "state_province");
		level = hierarchy.newLevel("City", "city");
		level = hierarchy.newLevel("Name", "name");
		dimension = newDimension("Education Level");
		hierarchy = dimension.newHierarchy(null, true, "SELECT * FROM \"customer\"", "customer_id", "customer_id");
		level = hierarchy.newLevel("Education Level", "education");
		dimension = newDimension("Gender");
		hierarchy = dimension.newHierarchy(null, true, "SELECT * FROM \"customer\"", "customer_id", "customer_id");
		level = hierarchy.newLevel("Gender", "gender");
		dimension = newDimension("Marital Status");
		hierarchy = dimension.newHierarchy(null, true, "SELECT * FROM \"customer\"", "customer_id", "customer_id");
		level = hierarchy.newLevel("Marital Status", "marital_status");
		dimension = newDimension("Yearly Income");
		hierarchy = dimension.newHierarchy(null, true, "SELECT * FROM \"customer\"", "customer_id", "customer_id");
		level = hierarchy.newLevel("Yearly Income", "yearly_income");

		this.measuresHierarchy.memberReader = new CacheMemberReader(
			new MeasureMemberSource(
				measuresHierarchy,
				new RolapMember[] {
					new RolapStoredMeasure(null, measuresLevel, "Unit Sales", "unit_sales", "sum", "#"),
					new RolapStoredMeasure(null, measuresLevel, "Store Cost", "store_cost", "sum", "Currency"),
					new RolapStoredMeasure(null, measuresLevel, "Store Sales", "store_sales", "sum", "Currency")}));
		init();
	}

	private RolapDimension newDimension(String name)
	{
		RolapDimension dimension = new RolapDimension(this, dimensions.length, name);
		this.dimensions = (RolapDimension[]) RolapUtil.addElement(
			this.dimensions, dimension);
		return dimension;
	}

	void init()
	{
		for (int i = 0; i < dimensions.length; i++) {
			((RolapDimension) dimensions[i]).init();
		}
		this.cellReader = AggregationManager.instance();
		if (false) {
			this.cellReader = new CachingCellReader(this, this.cellReader);
		}
		register();
	}

	void register()
	{
		Vector v = new Vector();
		Member[] measures = getMeasures();
		for (int i = 0; i < measures.length; i++) {
			if (measures[i] instanceof RolapStoredMeasure) {
				v.addElement(measures[i]);
			}
		}
		RolapStoredMeasure[] storedMeasures = new RolapStoredMeasure[v.size()];
		v.copyInto(storedMeasures);
		// create a star for each measure (todo: pool them)
		for (int i = 0; i < storedMeasures.length; i++) {
			RolapStoredMeasure storedMeasure = storedMeasures[i];
			RolapStar star = new RolapStar();
			star.jdbcConnection =
				((RolapConnection) getConnection()).jdbcConnection;
			Vector tablesVector = new Vector();
			// create a fact table
			star.factTable = new RolapStar.Table();
			star.factTable.star = star;
			star.factTable.alias = this.getAlias();
			star.factTable.setTable(this.factSchema, this.factTable);
			star.factTable.columns = new RolapStar.Column[1];
			RolapStar.Measure measure = new RolapStar.Measure();
			measure.table = star.factTable;
			measure.name = storedMeasure.column;
			measure.aggregator = storedMeasure.aggregator;
			measure.isNumeric = true;
			star.factTable.columns[0] = measure;
			// create dimension tables
			RolapDimension[] dimensions =
				(RolapDimension[]) this.getDimensions();
			for (int j = 0; j < dimensions.length; j++) {
				RolapDimension dimension = dimensions[j];
				RolapHierarchy[] hierarchies =
					(RolapHierarchy[]) dimension.getHierarchies();
				for (int k = 0; k < hierarchies.length; k++) {
					RolapHierarchy hierarchy = hierarchies[k];
					// assume there's one 'table' per hierarchy
					RolapStar.Table table = new RolapStar.Table();
					RolapLevel[] levels =
						(RolapLevel[]) hierarchy.getLevels();
					table.star = star;
					table.alias = hierarchy.getAlias();
                    if (hierarchy.getQuery() != null) {
                        table.setQuery(hierarchy.getQuery());
                    } else {
                        table.setTable(hierarchy.getSchema(), hierarchy.getTable());
                    }
					table.primaryKey = hierarchy.primaryKey;
					HierarchyUsage hierarchyUsage = hierarchy.getUsage(
						this.factSchema, this.factTable);
					table.foreignKey = hierarchyUsage.foreignKey;
					table.columns = new RolapStar.Column[levels.length];
					tablesVector.addElement(table);
					for (int l = 0; l < levels.length; l++) {
						RolapLevel level = levels[l];
						if (level.column == null) {
							continue;
						}
						RolapStar.Column column = new RolapStar.Column();
						column.table = table;
						column.name = level.column;
						column.isNumeric = (level.flags & RolapLevel.NUMERIC)
							!= 0;
						table.columns[l] = column;
						star.mapLevelToColumn.put(level, column);
					}
				}
			}
			star.tables = new RolapStar.Table[tablesVector.size()];
			tablesVector.copyInto(star.tables);
			storedMeasure.star = star;
		}
	}

	// implement NameResolver
	public Member lookupMemberFromCache(String s)
	{
		return (Member) mapNameToMember.get(s);
	}

	public void lookupMembers(
		Vector memberNames, Hashtable mapNameToMember)
	{
		throw new Error("unsupported");
	}
	public Member[] getMemberChildren(Member[] parentOlapMembers)
	{
		if (parentOlapMembers.length == 0) {
			return new RolapMember[0];
		}
		RolapHierarchy hierarchy = (RolapHierarchy)
			parentOlapMembers[0].getHierarchy();
		for (int i = 1; i < parentOlapMembers.length; i++) {
			Util.assertTrue(
				parentOlapMembers[i].getHierarchy() == hierarchy);
		}
		if (!(parentOlapMembers instanceof RolapMember[])) {
			Member[] old = parentOlapMembers;
			parentOlapMembers = new RolapMember[old.length];
			System.arraycopy(old, 0, parentOlapMembers, 0, old.length);
		}
		return hierarchy.memberReader.getMemberChildren(
			(RolapMember[]) parentOlapMembers);
	}
	public Member[] getMembersForQuery(
		String sQuery, Vector calcMembers)
	{
		throw new Error("unsupported");
	}
	String getAlias()
	{
		return "fact";
	}
    /**
	 * Returns whether this cube is virtual. We use the fact that virtual cubes
	 * do not have fact tables.
	 **/
    boolean isVirtual() {
		return factTable == null;
    }
};

class OldAvidCellReader implements CellReader
{
	OldAvidCellReader()
	{}

	// implement CellReader
	public Object get(Evaluator evaluator)
	{
		RolapCube cube = (RolapCube) evaluator.getCube();
		SqlQuery sqlQuery = new SqlQuery(null);
		RolapDimension[] dimensions = (RolapDimension[]) cube.getDimensions();
		for (int i = dimensions.length - 1; i >= 0; i--) {
			RolapMember member = (RolapMember) evaluator.getContext(dimensions[i]);
			RolapHierarchy hierarchy = (RolapHierarchy)	member.getHierarchy();
			hierarchy.memberReader.qualifyQuery(sqlQuery, member);
		}
		String sql = sqlQuery.toString();
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			if (RolapUtil.debugOut != null) {
				RolapUtil.debugOut.println(
					"AvidCellReader: executing sql [" + sql + "]");
			}
			java.sql.Connection connection =
				((RolapConnection) cube.getConnection()).jdbcConnection;
			statement = connection.createStatement();
			resultSet = statement.executeQuery(sql);
			Object o = null;
			if (resultSet.next()) {
				o = resultSet.getObject(1);
			}
			if (o == null) {
				o = Util.nullValue; // convert to placeholder
			}
			return o;
		} catch (SQLException e) {
			throw Util.getRes().newInternal(
				e, "while computing measure; sql=[" + sql + "]");
		} finally {
			try {
				if (resultSet != null) {
					resultSet.close();
				}
				if (statement != null) {
					statement.close();
				}
			} catch (SQLException e) {
				// ignore
			}
		}
	}
};

// End RolapCube.java
