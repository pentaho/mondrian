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
import mondrian.rolap.agg.AggregationManager;

import java.util.*;

/**
 * <code>RolapCube</code> implements {@link Cube} for a ROLAP database.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapCube extends CubeBase
{
	RolapSchema schema;
	RolapHierarchy measuresHierarchy;
	HashMap mapNameToMember = new HashMap();
	/** For SQL generator. Fact table. */
	MondrianDef.Relation fact;
	/** To access all measures stored in the fact table. */
	CellReader cellReader;
	/**
	 * Mapping such that
	 * <code>localDimensionOrdinals[dimension.globalOrdinal]</code> is equal to
	 * the ordinal of the dimension in this cube. See {@link
	 * RolapDimension#topic_ordinals}
	 */
	int[] localDimensionOrdinals;

	RolapCube(
		RolapSchema schema, MondrianDef.Schema xmlSchema,
		MondrianDef.Cube xmlCube)
	{
		this.schema = schema;
		this.name = xmlCube.name;
		this.fact = xmlCube.fact;
		if (fact.getAlias() == null) {
			throw Util.newError(
					"Must specify alias for fact table of cube " +
					getUniqueName());
		}
		this.dimensions = new RolapDimension[xmlCube.dimensions.length + 1];
		RolapDimension measuresDimension = new RolapDimension(schema, Dimension.MEASURES_NAME, 0);
		this.dimensions[0] = measuresDimension;
		this.measuresHierarchy = measuresDimension.newHierarchy(null, false, null, null, null);
		RolapLevel measuresLevel = this.measuresHierarchy.newLevel("MeasuresLevel");
		for (int i = 0; i < xmlCube.dimensions.length; i++) {
			MondrianDef.CubeDimension xmlCubeDimension = xmlCube.dimensions[i];
			MondrianDef.Dimension xmlDimension = xmlCubeDimension.getDimension(xmlSchema);
			dimensions[i + 1] = new RolapDimension(schema, this, xmlDimension, xmlCubeDimension);
		}
		RolapMeasure measures[] = new RolapMeasure[
			xmlCube.measures.length];
		for (int i = 0; i < xmlCube.measures.length; i++) {
			MondrianDef.Measure xmlMeasure = xmlCube.measures[i];
			measures[i] = new RolapStoredMeasure(
					this, null, measuresLevel, xmlMeasure.name, xmlMeasure.formatString,
					xmlMeasure.column, xmlMeasure.aggregator);
		}
		this.measuresHierarchy.memberReader = new CacheMemberReader(
			new MeasureMemberSource(measuresHierarchy, measures));
		init();
	}

	/**
	 * Creates a <code>RolapCube</code> from a virtual cube.
	 **/
	RolapCube(
		RolapSchema schema, MondrianDef.Schema xmlSchema,
		MondrianDef.VirtualCube xmlVirtualCube)
	{
		this.schema = schema;
		this.name = xmlVirtualCube.name;
		this.fact = null;
		this.dimensions = new RolapDimension[
			xmlVirtualCube.dimensions.length + 1];
		RolapDimension measuresDimension = new RolapDimension(
			schema, Dimension.MEASURES_NAME, 0);
		this.dimensions[0] = measuresDimension;
		this.measuresHierarchy = measuresDimension.newHierarchy(null, false, null, null, null);
		this.measuresHierarchy.newLevel("MeasuresLevel");
		for (int i = 0; i < xmlVirtualCube.dimensions.length; i++) {
			MondrianDef.VirtualCubeDimension xmlCubeDimension =
				xmlVirtualCube.dimensions[i];
			MondrianDef.Dimension xmlDimension = xmlCubeDimension.getDimension(
				xmlSchema);
			dimensions[i + 1] = new RolapDimension(
					schema, this, xmlDimension, xmlCubeDimension);
		}
		RolapMeasure measures[] = new RolapMeasure[
			xmlVirtualCube.measures.length];
		for (int i = 0; i < xmlVirtualCube.measures.length; i++) {
			// Lookup a measure in an existing cube. (Don't know whether it
			// will confuse things that this measure still points to its 'real'
			// cube.)
			MondrianDef.VirtualCubeMeasure xmlMeasure =
				xmlVirtualCube.measures[i];
			Cube cube = schema.lookupCube(xmlMeasure.cubeName);
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

	RolapCube(RolapSchema schema)
	{
		this.schema = schema;
		this.name = "Sales";
		this.fact = new MondrianDef.Table(null, "sales_fact_1997", "fact");
		this.dimensions = new RolapDimension[0];
		RolapDimension dimension;
		RolapHierarchy hierarchy;
		RolapLevel level;
		dimension = newDimension("Measures");
		hierarchy = dimension.newHierarchy(null, false, null, null, null);
		this.measuresHierarchy = hierarchy;
		level = hierarchy.newLevel("MeasuresLevel");
		RolapLevel measuresLevel = level;
		dimension = newDimension("Store");
		hierarchy = dimension.newHierarchy(
			null, true, "SELECT * FROM \"store\"", "store_id", "store_id");
		level = hierarchy.newLevel("Store Country", "store", "store_country");
		level = hierarchy.newLevel("Store State", "store", "store_state");
		level = hierarchy.newLevel("Store City", "store", "store_city");
		level = hierarchy.newLevel("Store Name", "store", "store_name");
		dimension = newDimension("Time");
		hierarchy = dimension.newHierarchy(
			null, false,
			"SELECT * FROM \"time_by_day\"", "time_id", "time_id");
		level = hierarchy.newLevel("Year", "time_by_day", "the_year", null, RolapLevel.NUMERIC);
		level = hierarchy.newLevel("Quarter", "time_by_day", "quarter");
		level = hierarchy.newLevel("Month", "time_by_day", "month_of_year", null, RolapLevel.NUMERIC);
		dimension = newDimension("Product");
		hierarchy = dimension.newHierarchy(
			null, true,
			"SELECT * FROM \"product\", \"product_class\" " +
			"WHERE \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"",
			"product_id", "product_id");
		level = hierarchy.newLevel("Product Family", "product", "product_family");
		level = hierarchy.newLevel("Product Department", "product", "product_department");
		level = hierarchy.newLevel("Product Category", "product", "product_category");
		level = hierarchy.newLevel("Product Subcategory", "product", "product_subcategory");
		level = hierarchy.newLevel("Brand Name", "product", "brand_name");
		level = hierarchy.newLevel("Product Name", "product", "product_name");
		dimension = newDimension("Promotion Media");
		hierarchy = dimension.newHierarchy(null, true, "SELECT * FROM \"promotion\"", "promotion_id", "promotion_id");
		level = hierarchy.newLevel("Media Type", "promotion", "media_type");
		dimension = newDimension("Promotions");
		hierarchy = dimension.newHierarchy(null, true, "SELECT * FROM \"promotion\"", "promotion_id", "promotion_id");
		level = hierarchy.newLevel("Promotion Name", "promotion", "promotion_name");
		dimension = newDimension("Customers");
		hierarchy = dimension.newHierarchy(null, true, "SELECT *, \"fname\" + ' ' + \"lname\" as \"name\" FROM \"customer\"", "customer_id", "customer_id");
		level = hierarchy.newLevel("Country", "customer", "country");
		level = hierarchy.newLevel("State Province", "customer", "state_province");
		level = hierarchy.newLevel("City", "customer", "city");
		level = hierarchy.newLevel("Name", "customer", "name");
		dimension = newDimension("Education Level");
		hierarchy = dimension.newHierarchy(null, true, "SELECT * FROM \"customer\"", "customer_id", "customer_id");
		level = hierarchy.newLevel("Education Level", "customer", "education");
		dimension = newDimension("Gender");
		hierarchy = dimension.newHierarchy(null, true, "SELECT * FROM \"customer\"", "customer_id", "customer_id");
		level = hierarchy.newLevel("Gender", "customer", "gender");
		dimension = newDimension("Marital Status");
		hierarchy = dimension.newHierarchy(null, true, "SELECT * FROM \"customer\"", "customer_id", "customer_id");
		level = hierarchy.newLevel("Marital Status", "customer", "marital_status");
		dimension = newDimension("Yearly Income");
		hierarchy = dimension.newHierarchy(null, true, "SELECT * FROM \"customer\"", "customer_id", "customer_id");
		level = hierarchy.newLevel("Yearly Income", "customer", "yearly_income");

		this.measuresHierarchy.memberReader = new CacheMemberReader(
			new MeasureMemberSource(
				measuresHierarchy,
				new RolapMember[] {
					new RolapStoredMeasure(this, null, measuresLevel, "Unit Sales", "unit_sales", "sum", "#"),
					new RolapStoredMeasure(this, null, measuresLevel, "Store Cost", "store_cost", "sum", "Currency"),
					new RolapStoredMeasure(this, null, measuresLevel, "Store Sales", "store_sales", "sum", "Currency")}));
		init();
	}

	private RolapDimension newDimension(String name)
	{
		RolapDimension dimension = new RolapDimension(schema, name, RolapDimension.nextOrdinal++);
		this.dimensions = (RolapDimension[]) RolapUtil.addElement(
			this.dimensions, dimension);
		return dimension;
	}

	void init()
	{
		int max = -1;
		for (int i = 0; i < dimensions.length; i++) {
			final RolapDimension dimension = (RolapDimension) dimensions[i];
			dimension.init(this);
			max = Math.max(max, dimension.getGlobalOrdinal());
		}
		this.localDimensionOrdinals = new int[max + 1];
		Arrays.fill(localDimensionOrdinals, -1);
		for (int i = 0; i < dimensions.length; i++) {
			final RolapDimension dimension = (RolapDimension) dimensions[i];
			final int globalOrdinal = dimension.getGlobalOrdinal();
			Util.assertTrue(
					localDimensionOrdinals[globalOrdinal] == -1,
					"duplicate dimension globalOrdinal " + globalOrdinal);
			localDimensionOrdinals[globalOrdinal] = i;
		}
		this.cellReader = AggregationManager.instance();
		register();
	}

	void register()
	{
		if (isVirtual()) {
			return;
		}
		ArrayList list = new ArrayList();
		Member[] measures = getMeasures();
		for (int i = 0; i < measures.length; i++) {
			if (measures[i] instanceof RolapStoredMeasure) {
				list.add(measures[i]);
			}
		}
		RolapStoredMeasure[] storedMeasures = (RolapStoredMeasure[])
				list.toArray(new RolapStoredMeasure[list.size()]);
		RolapStar star = RolapStar.Pool.instance().getOrCreateStar(
				schema, this.fact);
		// create measures (and stars for them, if necessary)
		for (int i = 0; i < storedMeasures.length; i++) {
			RolapStoredMeasure storedMeasure = storedMeasures[i];
			RolapStar.Measure measure = new RolapStar.Measure();
			measure.table = star.factTable;
			measure.expression = storedMeasure.expression;
			measure.aggregator = storedMeasure.aggregator;
			measure.isNumeric = true;
			storedMeasure.starMeasure = measure; // reverse mapping
			star.factTable.columns.add(measure);
		}
		// create dimension tables
		RolapDimension[] dimensions = (RolapDimension[]) this.getDimensions();
		for (int j = 0; j < dimensions.length; j++) {
			registerDimension(dimensions[j]);
		}
	}

	private void registerDimension(RolapDimension dimension) {
		RolapStar star = RolapStar.Pool.instance().getOrCreateStar(
				schema, this.fact);
		RolapHierarchy[] hierarchies = (RolapHierarchy[])
				dimension.getHierarchies();
		for (int k = 0; k < hierarchies.length; k++) {
			RolapHierarchy hierarchy = hierarchies[k];
			HierarchyUsage hierarchyUsage = schema.getUsage(hierarchy,this);
			MondrianDef.Relation relation = hierarchy.getRelation();
			if (relation == null) {
				continue; // e.g. [Measures] hierarchy
			}
			RolapStar.Table table = star.factTable;
			if (!relation.equals(table.relation)) {
				RolapStar.Condition joinCondition = new RolapStar.Condition(
						table.getAlias(), hierarchyUsage.foreignKey,
						hierarchyUsage.primaryKeyTable.getAlias(),
						hierarchyUsage.primaryKey);
				table = table.addJoin(relation, joinCondition);
			}
			RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
			for (int l = 0; l < levels.length; l++) {
				RolapLevel level = levels[l];
				if (level.nameExp == null) {
					continue;
				} else {
					RolapStar.Column column = new RolapStar.Column();
					if (level.nameExp instanceof MondrianDef.Column) {
						String tableName = ((MondrianDef.Column) level.nameExp).table;
						column.table = table.findAncestor(tableName);
						if (column.table == null) {
							throw Util.newError(
									"Level '" + level.getUniqueName() +
									"' of cube '" + this +
									"' is invalid: table '" + tableName +
									"' is not found in current scope");
						}
					} else {
						column.table = table;
					}
					column.expression = level.nameExp;
					column.isNumeric = (level.flags & RolapLevel.NUMERIC) != 0;
					table.columns.add(column);
					star.mapLevelToColumn.put(level, column);
				}
			}
		}
	}

	// implement NameResolver
	public Member lookupMemberFromCache(String s)
	{
		return (Member) mapNameToMember.get(s);
	}

    public void lookupMembers(Collection memberNames, Map mapNameToMember) {
		throw new UnsupportedOperationException();
    }

    public Member[] getMembersForQuery(String query, List calcMembers) {
        throw new UnsupportedOperationException();
    }

	MondrianDef.Relation getFact() {
		return fact;
	}

	String getAlias() {
		return fact.getAlias();
	}
    /**
	 * Returns whether this cube is virtual. We use the fact that virtual cubes
	 * do not have fact tables.
	 **/
    boolean isVirtual() {
		return fact == null;
    }

	RolapDimension createDimension(MondrianDef.CubeDimension xmlCubeDimension) {
		final RolapDimension dimension = new RolapDimension(
						schema, this, (MondrianDef.Dimension) xmlCubeDimension, xmlCubeDimension);
		dimension.init(this);
		// add to dimensions array
		final RolapDimension[] newDimensions = new RolapDimension[dimensions.length + 1];
		System.arraycopy(dimensions, 0, newDimensions, 0, dimensions.length);
		final int localOrdinal = newDimensions.length - 1;
		newDimensions[localOrdinal] = dimension;
		// write arrays into members; todo: prevent threading issues
		this.dimensions = newDimensions;
		// add to ordinals array
		final int globalOrdinal = dimension.getGlobalOrdinal();
		if (globalOrdinal >= localDimensionOrdinals.length) {
			int[] newLocalDimensionOrdinals = new int[globalOrdinal + 1];
			System.arraycopy(localDimensionOrdinals, 0,
					newLocalDimensionOrdinals, 0, localDimensionOrdinals.length);
			Arrays.fill(newLocalDimensionOrdinals,
					localDimensionOrdinals.length,
					newLocalDimensionOrdinals.length, -1);
			this.localDimensionOrdinals = newLocalDimensionOrdinals;
		}
		Util.assertTrue(localDimensionOrdinals[globalOrdinal] == -1);
		localDimensionOrdinals[globalOrdinal] = localOrdinal;
		registerDimension(dimension);
		return dimension;
	}
}

// End RolapCube.java
