/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;

import org.apache.log4j.Logger;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * <code>RolapLevel</code> implements {@link Level} for a ROLAP database.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
public class RolapLevel extends LevelBase {

    private static final Logger LOGGER = Logger.getLogger(RolapEvaluator.class);

    public static RolapLevel lookupLevel(RolapLevel[] levels, String levelName) {
        for (int i = 0; i < levels.length; i++) {
            RolapLevel level = levels[i];
            if (level.getName().equals(levelName)) {
                return level;
            }
        }
        return null;
    }

    static final int NUMERIC = 1;
    static final int ALL = 2;
    static final int UNIQUE = 4;

    /** The column or expression which yields the level's key. */
    private final MondrianDef.Expression keyExp;
    /** The column or expression which yields the level's ordinal. */
    private final MondrianDef.Expression ordinalExp;
    /** For SQL generator. Whether values of "column" are unique globally
     * unique (as opposed to unique only within the context of the parent
     * member). **/
    private final boolean unique;
    private final int flags;
    private final RolapProperty[] properties;
    private final RolapProperty[] inheritedProperties;

    /**
     * Ths expression which gives the name of members of this level. If null,
     * members are named using the key expression.
     */
    private final MondrianDef.Expression nameExp;
    /** The expression which joins to the parent member in a parent-child
     * hierarchy, or null if this is a regular hierarchy. */
    private final MondrianDef.Expression parentExp;
    /** Value which indicates a null parent in a parent-child hierarchy. */
    private final String nullParentValue;
    /** For a parent-child hierarchy with a closure provided by the schema,
     * the equivalent level in the closed hierarchy; otherwise null */
    private RolapLevel closedPeer;

    /** Condition under which members are hidden. */
    private final HideMemberCondition hideMemberCondition;
    private final MondrianDef.Closure xmlClosure;

    /**
     * Creates a level.
     *
     * @pre parentExp != null || nullParentValue == null
     * @pre properties != null
     * @pre levelType != null
     * @pre hideMemberCondition != null
     */
    RolapLevel(RolapHierarchy hierarchy,
        int depth,
        String name,
        MondrianDef.Expression keyExp,
        MondrianDef.Expression nameExp,
        MondrianDef.Expression ordinalExp,
        MondrianDef.Expression parentExp,
        String nullParentValue,
        MondrianDef.Closure xmlClosure,
        RolapProperty[] properties,
        int flags,
        HideMemberCondition
        hideMemberCondition,
        LevelType levelType)
    {
        super(hierarchy, name, depth, levelType);

        Util.assertPrecondition(properties != null, "properties != null");
        Util.assertPrecondition(hideMemberCondition != null,
                "hideMemberCondition != null");
        Util.assertPrecondition(levelType != null, "levelType != null");

        if (keyExp instanceof MondrianDef.Column) {
            checkColumn((MondrianDef.Column) keyExp);
        }
        this.flags = flags;
        final boolean isAll = (flags & ALL) == ALL;
        this.unique = (flags & UNIQUE) == UNIQUE;
        this.keyExp = keyExp;
        if (nameExp != null) {
            if (nameExp instanceof MondrianDef.Column) {
                checkColumn((MondrianDef.Column) nameExp);
            }
        }
        this.nameExp = nameExp;
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
        this.xmlClosure = xmlClosure;
        for (int i = 0; i < properties.length; i++) {
            RolapProperty property = properties[i];
            if (property.getExp() instanceof MondrianDef.Column) {
                checkColumn((MondrianDef.Column) property.getExp());
            }
        }
        this.properties = properties;
        List list = new ArrayList();
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

        Dimension dim = hierarchy.getDimension();
        if (dim.getDimensionType() == DimensionType.TimeDimension) {
            if (!levelType.isTime() && !isAll) {
                throw MondrianResource.instance()
                        .newNonTimeLevelInTimeHierarchy(getUniqueName());
           }
        } else if (dim.getDimensionType() == null) {
            // there was no dimension type assigned to the dimension
            // - check later
        } else {
            if (levelType.isTime()) {
                throw MondrianResource.instance()
                        .newTimeLevelInNonTimeHierarchy(getUniqueName());
            }
        }
        this.hideMemberCondition = hideMemberCondition;
        this.closedPeer = null;
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    String getTableName() {
        String tableName = null;

        MondrianDef.Expression expr = getKeyExp();
        if (expr instanceof MondrianDef.Column) {
            MondrianDef.Column mc = (MondrianDef.Column) expr;
            tableName = mc.getTableAlias();
        }
        return tableName;
    }

    public MondrianDef.Expression getKeyExp() {
        return keyExp;
    }
    MondrianDef.Expression getOrdinalExp() {
        return ordinalExp;
    }
    int getFlags() {
        return flags;
    }
    HideMemberCondition getHideMemberCondition() {
        return hideMemberCondition;
    }
    boolean isUnique() {
        return unique;
    }
    RolapProperty[] getRolapProperties() {
        return properties;
    }
    String getNullParentValue() {
        return nullParentValue;
    }
    MondrianDef.Expression getParentExp() {
        return parentExp;
    }
    MondrianDef.Expression getNameExp() {
        return nameExp;
    }

    private Property lookupProperty(List list, String propertyName) {
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

    RolapLevel(RolapHierarchy hierarchy, int depth, MondrianDef.Level xmlLevel) {
        this(
            hierarchy, depth, xmlLevel.name, xmlLevel.getKeyExp(),
            xmlLevel.getNameExp(), xmlLevel.getOrdinalExp(),
            xmlLevel.getParentExp(), xmlLevel.nullParentValue,
            xmlLevel.closure, createProperties(xmlLevel),
            (xmlLevel.type.equals("Numeric") ? NUMERIC : 0) |
            (xmlLevel.uniqueMembers.booleanValue() ? UNIQUE : 0),
            HideMemberCondition.lookup(xmlLevel.hideMemberIf),
            LevelType.lookup(xmlLevel.levelType));

        if (!Util.isEmpty(xmlLevel.caption)) {
            setCaption(xmlLevel.caption);
        }
        if (!Util.isEmpty(xmlLevel.formatter)) {
            // there is a special member formatter class
            try {
                Class clazz = Class.forName(xmlLevel.formatter);
                Constructor ctor = clazz.getConstructor(new Class[0]);
                memberFormatter = (MemberFormatter) ctor.newInstance(new Object[0]);
            } catch (Exception e) {
                throw MondrianResource.instance().newMemberFormatterLoadFailed(
                    xmlLevel.formatter, getUniqueName(), e);
            }
        }
    }

    // helper for constructor
    private static RolapProperty[] createProperties(
            MondrianDef.Level xmlLevel) {
        List list = new ArrayList();
        final MondrianDef.Expression nameExp = xmlLevel.getNameExp();
        if (nameExp != null) {
            list.add(
                    new RolapProperty(
                            Property.NAME.name, Property.TYPE_STRING,
                            nameExp, null, null));
        }
        for (int i = 0; i < xmlLevel.properties.length; i++) {
            MondrianDef.Property property = xmlLevel.properties[i];
            list.add(new RolapProperty(
                    property.name,
                    convertPropertyTypeNameToCode(property.type),
                    xmlLevel.getPropertyExp(i), property.formatter, property.caption));
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

    void init(RolapCube cube, MondrianDef.CubeDimension xmlDimension) {
        if (xmlClosure != null) {
            final RolapDimension dimension = ((RolapHierarchy) hierarchy)
                .createClosedPeerDimension(this, xmlClosure, cube, xmlDimension);
            dimension.init(cube, xmlDimension);
            cube.registerDimension(dimension);
            this.closedPeer = (RolapLevel) dimension.getHierarchies()[0].getLevels()[1];
        }
    }

    public boolean isAll() {
        return hierarchy.hasAll() && (depth == 0);
    }

    public boolean areMembersUnique() {
        return (depth == 0) || (depth == 1) && hierarchy.hasAll();
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
            return (HideMemberCondition) enumeration.getValue(s, true);
        }
    }

    public OlapElement lookupChild(SchemaReader schemaReader, String name) {
        Member[] levelMembers = schemaReader.getLevelMembers(this);

        for (int idx = 0; idx < levelMembers.length; idx++) {
            if (levelMembers[idx].getName().equals(name)) {
                return levelMembers[idx];
            }
        }

        return null;
    }


    /** true when the level is part of a parent/child hierarchy and has an equivalent
     * closed level
     */
    boolean hasClosedPeer() {
        return (parentExp != null) && (closedPeer != null);
    }


    /**
     * When the level is part of a parent/child hierarchy and was provided with
     * a closure, returns the equivalent closed level.
     */
    RolapLevel getClosedPeer() {
        return closedPeer;
    }


}

// End RolapLevel.java
