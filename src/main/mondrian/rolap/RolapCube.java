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
import mondrian.rolap.agg.AggregationManager;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

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
	/** Schema reader which can see this cube and nothing else. */
	private SchemaReader schemaReader;

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
		RolapDimension measuresDimension = new RolapDimension(schema, Dimension.MEASURES_NAME, 0, DimensionType.StandardDimension);
		this.dimensions[0] = measuresDimension;
		this.measuresHierarchy = measuresDimension.newHierarchy(null, false);
		RolapLevel measuresLevel = this.measuresHierarchy.newLevel("MeasuresLevel", 0);
		for (int i = 0; i < xmlCube.dimensions.length; i++) {
			MondrianDef.CubeDimension xmlCubeDimension = xmlCube.dimensions[i];
			MondrianDef.Dimension xmlDimension = xmlCubeDimension.getDimension(xmlSchema);
			dimensions[i + 1] = new RolapDimension(schema, this, xmlDimension, xmlCubeDimension);
		}
		RolapStoredMeasure measures[] = new RolapStoredMeasure[
			xmlCube.measures.length];
		for (int i = 0; i < xmlCube.measures.length; i++) {
			MondrianDef.Measure xmlMeasure = xmlCube.measures[i];
			measures[i] = new RolapStoredMeasure(
					this, null, measuresLevel, xmlMeasure.name, xmlMeasure.formatString,
					xmlMeasure.column, xmlMeasure.aggregator);

			if (xmlMeasure.formatter != null && xmlMeasure.formatter.length() > 0) {
				// there is a special cell formatter class
				try {
					Class clazz = Class.forName(xmlMeasure.formatter);
					Constructor ctor = clazz.getConstructor(new Class[0]);
					CellFormatter cellFormatter = (CellFormatter) ctor.newInstance(new Object[0]);
					measures[i].setFormatter(cellFormatter);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
	
			if (xmlMeasure.caption != null && xmlMeasure.caption.length() > 0) {
				// there is a special caption string
				measures[i].setProperty("$caption", xmlMeasure.caption);
			}
				
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
			schema, Dimension.MEASURES_NAME, 0, DimensionType.StandardDimension);
		this.dimensions[0] = measuresDimension;
		this.measuresHierarchy = measuresDimension.newHierarchy(null, false);
		this.measuresHierarchy.newLevel("MeasuresLevel", 0);
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
			RolapCube cube = (RolapCube) schema.lookupCube(xmlMeasure.cubeName);
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

    public Schema getSchema() {
        return schema;
    }

	/**
	 * Returns the schema reader which enforces the appropriate access-control
	 * context.
	 *
	 * @post return != null
	 * @see #getSchemaReader(Role)
	 */
	synchronized SchemaReader getSchemaReader() {
		if (schemaReader == null) {
			schemaReader = getSchemaReader(null);
		}
		return schemaReader;
	}

	public SchemaReader getSchemaReader(Role role) {
		if (role == null) {
			role = schema.defaultRole.makeMutableClone();
			role.grant(this, Access.ALL);
		}
		return new RolapSchemaReader(role, schema) {
			public Cube getCube() {
				return RolapCube.this;
			}
		};
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
		RolapStar star = schema.getRolapStarRegistry().getOrCreateStar(
				this.fact);
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
            star.mapColumnToName.put(measure, storedMeasure.getName());
		}
		// create dimension tables
		RolapDimension[] dimensions = (RolapDimension[]) this.getDimensions();
		for (int j = 0; j < dimensions.length; j++) {
			registerDimension(dimensions[j]);
		}
	}

	private void registerDimension(RolapDimension dimension) {
		RolapStar star = schema.getRolapStarRegistry().getOrCreateStar(
				this.fact);
		RolapHierarchy[] hierarchies = (RolapHierarchy[])
				dimension.getHierarchies();
		HashMap mapLevelToColumn = (HashMap) star.mapCubeToMapLevelToColumn.get(this);
		if (mapLevelToColumn == null) {
			mapLevelToColumn = new HashMap();
			star.mapCubeToMapLevelToColumn.put(this, mapLevelToColumn);
		}
		for (int k = 0; k < hierarchies.length; k++) {
			RolapHierarchy hierarchy = hierarchies[k];
			HierarchyUsage hierarchyUsage = schema.getUsage(hierarchy,this);
			MondrianDef.Relation relation = hierarchy.getRelation();
			if (relation == null) {
				continue; // e.g. [Measures] hierarchy
			}
			RolapStar.Table table = star.factTable;
			if (!relation.equals(table.relation)) {
                // HierarchyUsage should have checked this.
                Util.assertTrue(hierarchyUsage.foreignKey != null);
				RolapStar.Condition joinCondition = new RolapStar.Condition(
						new MondrianDef.Column(table.getAlias(), hierarchyUsage.foreignKey),
						hierarchyUsage.joinExp);
				table = table.addJoin(relation, joinCondition);
			}
			RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
			for (int l = 0; l < levels.length; l++) {
				RolapLevel level = levels[l];
				if (level.keyExp == null) {
					continue;
				} else {
					RolapStar.Column column = new RolapStar.Column();
					if (level.keyExp instanceof MondrianDef.Column) {
						String tableName = ((MondrianDef.Column) level.keyExp).table;
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
					column.expression = level.keyExp;
					column.isNumeric = (level.flags & RolapLevel.NUMERIC) != 0;
					table.columns.add(column);
					mapLevelToColumn.put(level, column);
                    star.mapColumnToName.put(column, level.getName());
				}
			}
		}
	}

	public Member[] getMembersForQuery(String query, List calcMembers) {
        throw new UnsupportedOperationException();
    }

	Member[] getMeasures() {
		return getSchemaReader().getLevelMembers(dimensions[0].getHierarchies()[0].getLevels()[0]);
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

	// implement NameResolver
	public OlapElement lookupChild(OlapElement parent, String s) {
		// use OlapElement's virtual lookup
		return parent.lookupChild(getSchemaReader(), s);
	}
	
	/**
	 * get the measures hierarchy
	 */
	public Hierarchy getMeasuresHierarchy(){
		return measuresHierarchy;
	}
	
}

// End RolapCube.java
