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

import java.util.ArrayList;
import java.util.Iterator;

/**
 * <code>RolapLevel</code> implements {@link Level} for a ROLAP database.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapLevel extends LevelBase
{
	/** The column or expression which yields the level's name. */
	MondrianDef.Expression nameExp;
	/** The column or expression which yields the level's ordinal. */
	MondrianDef.Expression ordinalExp;
	/** For SQL generator. Whether values of "column" are unique globally
	 * unique (as opposed to unique only within the context of the parent
	 * member). **/
	boolean unique;
	int flags;
	static final int NUMERIC = 1;
	static final int ALL = 2;
	RolapProperty[] properties;
	RolapProperty[] inheritedProperties;

	/**
	 * Creates a level.
	 *
	 * @pre properties != null
	 */
	RolapLevel(
			RolapHierarchy hierarchy, int depth, String name,
			MondrianDef.Expression nameExp, MondrianDef.Expression ordinalExp,
			RolapProperty[] properties, int flags) {
		Util.assertPrecondition(properties != null);
		this.hierarchy = hierarchy;
		this.name = name;
		this.uniqueName = Util.makeFqName(hierarchy, name);
		if (nameExp instanceof MondrianDef.Column) {
			checkColumn((MondrianDef.Column) nameExp);
		}
		this.nameExp = nameExp;
		if (ordinalExp instanceof MondrianDef.Column) {
			checkColumn((MondrianDef.Column) ordinalExp);
		}
		this.ordinalExp = ordinalExp;
		if (ordinalExp == null) {
			this.ordinalExp = nameExp;
		}
		for (int i = 0; i < properties.length; i++) {
			RolapProperty property = properties[i];
			if (property.exp instanceof MondrianDef.Column) {
				checkColumn((MondrianDef.Column) property.exp);
			}
		}
		this.properties = properties;
		ArrayList list = new ArrayList();
		for (Level level = this; level != null;
				level = level.getParentLevel()) {
			final Property[] levelProperties = level.getProperties();
			for (int i = 0; i < levelProperties.length; i++) {
				final Property levelProperty = levelProperties[i];
				Property existingProperty = lookupProperty(
						list, levelProperty.getName());
				if (existingProperty == null) {
					list.add(levelProperty);
				} else if (existingProperty.getType() !=
						levelProperty.getType()) {
					throw Util.newError(
							"Property " + this.getName() + "." +
							levelProperty.getName() + " overrides a " +
							"property with the same name but different type");
				}
			}
		}
		this.inheritedProperties = (RolapProperty[]) list.toArray(
				new RolapProperty[0]);
		this.flags = flags;
		this.depth = depth;
		this.levelType = Level.STANDARD;
		if (hierarchy.getDimension().getDimensionType() == Dimension.TIME) {
			if (name.equals("Year")) {
				this.levelType = Level.YEARS;
			} else if (name.equals("Quarter")) {
				this.levelType = Level.QUARTERS;
			} else if (name.equals("Month")) {
				this.levelType = Level.MONTHS;
			}
		}
	}

	private Property lookupProperty(ArrayList list, String propertyName) {
		Property existingProperty = null;
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			Property property = (Property) iterator.next();
			if (property.getName().equals(propertyName)) {
				existingProperty = property;
				break;
			}
		}
		return existingProperty;
	}

	RolapLevel(RolapHierarchy hierarchy, int depth, MondrianDef.Level xmlLevel)
	{
		this(
				hierarchy, depth, xmlLevel.name, xmlLevel.getNameExp(),
				xmlLevel.getOrdinalExp(), createProperties(xmlLevel),
				xmlLevel.type.equals("Numeric") ? NUMERIC : 0);
	}

	static RolapProperty[] createProperties(MondrianDef.Level xmlLevel) {
		RolapProperty[] properties = new RolapProperty[
				xmlLevel.properties.length];
		for (int i = 0; i < xmlLevel.properties.length; i++) {
			MondrianDef.Property property = xmlLevel.properties[i];
			properties[i] = new RolapProperty(
					property.name,
					convertPropertyTypeNameToCode(property.type),
					xmlLevel.getPropertyExp(i));
		}
		return properties;
	}

	private static int convertPropertyTypeNameToCode(String type) {
		if (type.equals("String")) {
			return Property.TYPE_STRING;
		} else if (type.equals("Numeric")) {
			return Property.TYPE_NUMERIC;
		} else if (type.equals("Boolean")) {
			return Property.TYPE_BOOLEAN;
		} else {
			throw Util.newError("Unknown property type '" + type + "'");
		}
	}

	private void checkColumn(MondrianDef.Column nameColumn) {
		final RolapHierarchy rolapHierarchy = (RolapHierarchy) hierarchy;
		if (nameColumn.table == null) {
			final MondrianDef.Relation table = rolapHierarchy.getUniqueTable();
			if (table == null) {
				throw Util.newError(
						"must specify a table for level " +
						getUniqueName() +
						" because hierarchy has more than one table");
			}
			nameColumn.table = table.getAlias();
		} else {
			Util.assertTrue(rolapHierarchy.tableExists(nameColumn.table));
		}
	}

	void init()
	{
	}

	public Member[] getMembers() {
		return ((RolapHierarchy) hierarchy).memberReader.getMembersInLevel(
			this, 0, Integer.MAX_VALUE);
	}

	public boolean isAll() {
		return hierarchy.hasAll() && depth == 0;
	}

	public boolean areMembersUnique() {
		return depth == 0 ||
			depth == 1 && hierarchy.hasAll();
	}

	public String getTableAlias() {
		return nameExp.getTableAlias();
	}

	public Property[] getProperties() {
		return properties;
	}

	public Property[] getInheritedProperties() {
		return inheritedProperties;
	}
}

// End RolapLevel.java
