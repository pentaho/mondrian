/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2004 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;

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
	/** The column or expression which yields the level's key. */
	final MondrianDef.Expression keyExp;
	/** The column or expression which yields the level's ordinal. */
	final MondrianDef.Expression ordinalExp;
	/** For SQL generator. Whether values of "column" are unique globally
	 * unique (as opposed to unique only within the context of the parent
	 * member). **/
	final boolean unique;
	int flags;
	static final int NUMERIC = 1;
	static final int ALL = 2;
	static final int UNIQUE = 4;
	RolapProperty[] properties;
	RolapProperty[] inheritedProperties;
	/** The expression which joins to the parent member in a parent-child
	 * hierarchy, or null if this is a regular hierarchy. */
	final MondrianDef.Expression parentExp;
	/** Value which indicates a null parent in a parent-child hierarchy. */
	String nullParentValue;
    /** Condition under which members are hidden. */
    final HideMemberCondition hideMemberCondition;

    /**
	 * Creates a level.
	 *
	 * @pre parentExp != null || nullParentValue == null
	 * @pre properties != null
     * @pre levelType != null
     * @pre hideMemberCondition != null
	 */
	RolapLevel(
			RolapHierarchy hierarchy, int depth, String name,
			MondrianDef.Expression keyExp,
			MondrianDef.Expression ordinalExp,
			MondrianDef.Expression parentExp, String nullParentValue,
			RolapProperty[] properties,
            int flags,
            HideMemberCondition hideMemberCondition,
            LevelType levelType) {
        Util.assertPrecondition(properties != null, "properties != null");
        Util.assertPrecondition(hideMemberCondition != null,
                "hideMemberCondition != null");
        Util.assertPrecondition(levelType != null, "levelType != null");
		this.hierarchy = hierarchy;
		this.name = name;
		this.uniqueName = Util.makeFqName(hierarchy, name);
		if (keyExp instanceof MondrianDef.Column) {
			checkColumn((MondrianDef.Column) keyExp);
		}
		this.flags = flags;
		final boolean isAll = (flags & ALL) == ALL;
		this.unique = (flags & UNIQUE) == UNIQUE;
		this.depth = depth;
		this.keyExp = keyExp;
		if (ordinalExp != null) {
			if (ordinalExp instanceof MondrianDef.Column) {
				checkColumn((MondrianDef.Column) ordinalExp);
			}
			this.ordinalExp = ordinalExp;
		} else {
			this.ordinalExp = this.keyExp;
		}
		this.parentExp = parentExp;
		if (parentExp != null) {
			Util.assertTrue(!isAll, "'All' level '" + this + "' must not be parent-child");
			Util.assertTrue(unique, "Parent-child level '" + this + "' must have uniqueMembers=\"true\"");
		}
		this.nullParentValue = nullParentValue;
		Util.assertPrecondition(parentExp != null || nullParentValue == null,
                "parentExp != null || nullParentValue == null");
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
				RolapProperty.emptyArray);
        this.levelType = levelType;
		if (hierarchy.getDimension().getDimensionType() == Dimension.TIME) {
            if (!levelType.isTime()) {
                throw MondrianResource.instance()
                        .newNonTimeLevelInTimeHierarchy(getUniqueName());
            }
        } else {
            if (levelType.isTime()) {
                throw MondrianResource.instance()
                        .newTimeLevelInNonTimeHierarchy(getUniqueName());
            }
        }
        this.hideMemberCondition = hideMemberCondition;
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
				hierarchy, depth, xmlLevel.name, xmlLevel.getKeyExp(),
				xmlLevel.getOrdinalExp(),
				xmlLevel.getParentExp(), xmlLevel.nullParentValue,
				createProperties(xmlLevel),
				(xmlLevel.type.equals("Numeric") ? NUMERIC : 0) |
				(xmlLevel.uniqueMembers.booleanValue() ? UNIQUE : 0),
                HideMemberCondition.lookup(xmlLevel.hideMemberIf),
                LevelType.lookup(xmlLevel.levelType));
	}

    // helper for constructor
    private static RolapProperty[] createProperties(
            MondrianDef.Level xmlLevel) {
		ArrayList list = new ArrayList();
		final MondrianDef.Expression nameExp = xmlLevel.getNameExp();
		if (nameExp != null) {
			list.add(new RolapProperty(
					Property.PROPERTY_NAME, Property.TYPE_STRING, nameExp));
		}
		for (int i = 0; i < xmlLevel.properties.length; i++) {
			MondrianDef.Property property = xmlLevel.properties[i];
			list.add(new RolapProperty(
					property.name,
					convertPropertyTypeNameToCode(property.type),
					xmlLevel.getPropertyExp(i)));
		}
		return (RolapProperty[]) list.toArray(RolapProperty.emptyArray);
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

	public boolean isAll() {
		return hierarchy.hasAll() && depth == 0;
	}

	public boolean areMembersUnique() {
		return depth == 0 ||
			depth == 1 && hierarchy.hasAll();
	}

	public String getTableAlias() {
		return keyExp.getTableAlias();
	}

	public Property[] getProperties() {
		return properties;
	}

	public Property[] getInheritedProperties() {
		return inheritedProperties;
	}

    /**
     * Conditions under which a level's members may be hidden (thereby creating
     * a <dfn>ragged hierarchy</dfn>).
     */
    public static class HideMemberCondition extends EnumeratedValues.BasicValue {
        private HideMemberCondition(String name, int ordinal) {
            super(name, ordinal, null);
        }

        public static final int NeverORDINAL = 0;
        /** A member always appears. */
        public static final HideMemberCondition Never =
                new HideMemberCondition("Never", NeverORDINAL);
        public static final int IfBlankNameORDINAL = 1;
        /** A member doesn't appear if its name is null or empty. */
        public static final HideMemberCondition IfBlankName =
                new HideMemberCondition("IfBlankName", IfBlankNameORDINAL);
        public static final int IfParentsNameORDINAL = 2;
        /** A member appears unless its name matches its parent's. */
        public static final HideMemberCondition IfParentsName =
                new HideMemberCondition("IfParentsName", IfParentsNameORDINAL);
        public static final EnumeratedValues enumeration =
                new EnumeratedValues(
                        new HideMemberCondition[] {
                            Never, IfBlankName, IfParentsName
                        }
                );
        public static HideMemberCondition lookup(String s) {
            return (HideMemberCondition) enumeration.getValue(s);
        }
    }

}

// End RolapLevel.java
