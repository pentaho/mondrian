/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.agg.MemberColumnConstraint;

import org.apache.log4j.Logger;
import java.lang.reflect.Constructor;
import java.util.*;

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
    /** The column or expression which yields the level members' caption. */
    private final MondrianDef.Expression captionExp;
    /** For SQL generator. Whether values of "column" are unique globally
     * unique (as opposed to unique only within the context of the parent
     * member). */
    private final boolean unique;
    private final boolean numeric;
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

    /** Condition under which members are hidden. */
    private final HideMemberCondition hideMemberCondition;
    private final MondrianDef.Closure xmlClosure;

    private LevelReader levelReader;

    /**
     * Creates a level.
     *
     * @pre parentExp != null || nullParentValue == null
     * @pre properties != null
     * @pre levelType != null
     * @pre hideMemberCondition != null
     */
    RolapLevel(
            RolapHierarchy hierarchy,
            int depth,
            String name,
            MondrianDef.Expression keyExp,
            MondrianDef.Expression nameExp,
            MondrianDef.Expression captionExp,
            MondrianDef.Expression ordinalExp,
            MondrianDef.Expression parentExp,
            String nullParentValue,
            MondrianDef.Closure xmlClosure,
            RolapProperty[] properties,
            int flags,
            HideMemberCondition
            hideMemberCondition,
            LevelType levelType, String approxRowCount)
    {
        super(hierarchy, name, depth, levelType);

        Util.assertPrecondition(properties != null, "properties != null");
        Util.assertPrecondition(hideMemberCondition != null,
                "hideMemberCondition != null");
        Util.assertPrecondition(levelType != null, "levelType != null");

        if (keyExp instanceof MondrianDef.Column) {
            checkColumn((MondrianDef.Column) keyExp);
        }
        this.approxRowCount = approxRowCount;
        this.flags = flags;
        final boolean isAll = (flags & ALL) == ALL;
        this.unique = (flags & UNIQUE) == UNIQUE;
        this.numeric = (flags & NUMERIC) == NUMERIC;
        this.keyExp = keyExp;
        if (nameExp != null) {
            if (nameExp instanceof MondrianDef.Column) {
                checkColumn((MondrianDef.Column) nameExp);
            }
        }
        this.nameExp = nameExp;
        if (captionExp != null) {
            if (captionExp instanceof MondrianDef.Column) {
                checkColumn((MondrianDef.Column) captionExp);
            }
        }
        this.captionExp = captionExp;
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
                        .NonTimeLevelInTimeHierarchy.ex(getUniqueName());
           }
        } else if (dim.getDimensionType() == null) {
            // there was no dimension type assigned to the dimension
            // - check later
        } else {
            if (levelType.isTime()) {
                throw MondrianResource.instance()
                        .TimeLevelInNonTimeHierarchy.ex(getUniqueName());
            }
        }
        this.hideMemberCondition = hideMemberCondition;
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

    LevelReader getLevelReader() {
        return levelReader;
    }

    public MondrianDef.Expression getKeyExp() {
        return keyExp;
    }

    MondrianDef.Expression getOrdinalExp() {
        return ordinalExp;
    }

    public MondrianDef.Expression getCaptionExp() {
        return captionExp;
    }
    public boolean hasCaptionColumn(){
        return captionExp != null;
    }
    int getFlags() {
        return flags;
    }
    HideMemberCondition getHideMemberCondition() {
        return hideMemberCondition;
    }
    public boolean isUnique() {
        return unique;
    }
    boolean isNumeric() {
        return numeric;
    }
    RolapProperty[] getRolapProperties() {
        return properties;
    }
    String getNullParentValue() {
        return nullParentValue;
    }

    /**
     * Returns whether this level is parent-child.
     */
    public boolean isParentChild() {
        return parentExp != null;
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
            xmlLevel.getNameExp(), xmlLevel.getCaptionExp(), xmlLevel.getOrdinalExp(),
            xmlLevel.getParentExp(), xmlLevel.nullParentValue,
            xmlLevel.closure, createProperties(xmlLevel),
            (xmlLevel.type.equals("Numeric") ? NUMERIC : 0) |
            (xmlLevel.uniqueMembers.booleanValue() ? UNIQUE : 0),
            HideMemberCondition.lookup(xmlLevel.hideMemberIf),
            LevelType.lookup(xmlLevel.levelType), xmlLevel.approxRowCount);

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
                throw MondrianResource.instance().MemberFormatterLoadFailed.ex(
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
            list.add(new RolapProperty(
                    Property.NAME.name, Property.TYPE_STRING,
                    nameExp, null, null));
        }
        for (int i = 0; i < xmlLevel.properties.length; i++) {
            MondrianDef.Property property = xmlLevel.properties[i];
            list.add(new RolapProperty(
                    property.name,
                    convertPropertyTypeNameToCode(property.type),
                    xmlLevel.getPropertyExp(i),
                    property.formatter, property.caption));
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
        if (isAll()) {
            this.levelReader = new AllLevelReaderImpl();
        } else if (levelType == LevelType.Null) {
            this.levelReader = new NullLevelReader();
        } else if (xmlClosure != null) {
            final RolapDimension dimension = ((RolapHierarchy) hierarchy)
                .createClosedPeerDimension(this, xmlClosure, cube, xmlDimension);

            dimension.init(cube, xmlDimension);
            cube.registerDimension(dimension);
            RolapLevel closedPeer =
                    (RolapLevel) dimension.getHierarchies()[0].getLevels()[1];
            this.levelReader = new ParentChildLevelReaderImpl(closedPeer);
        } else {
            this.levelReader = new RegularLevelReader();
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

    public String getApproxRowCount() {
        return approxRowCount;
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
        return lookupChild(schemaReader, name, MatchType.EXACT);
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, String name, int matchType)
    {
        Member[] levelMembers = schemaReader.getLevelMembers(this, true);
        int bestMatch = -1;
        for (int i = 0; i < levelMembers.length; i++) {
            int rc = levelMembers[i].getName().compareTo(name);
            if (rc == 0) {
                return levelMembers[i];
            }
            if (matchType == MatchType.BEFORE) {
                if (rc < 0 &&
                    (bestMatch == -1 ||
                    levelMembers[i].getName().compareTo(
                        levelMembers[bestMatch].getName()) > 0))
                {
                    bestMatch = i;
                }
            } else if (matchType == MatchType.AFTER) {
                if (rc > 0 &&
                    (bestMatch == -1 ||
                    levelMembers[i].getName().compareTo(
                        levelMembers[bestMatch].getName()) < 0))
                {
                    bestMatch = i;
                }
            }
        }
        if (matchType != MatchType.EXACT && bestMatch != -1) {
            return levelMembers[bestMatch];
        }
        return null;
    }

    /**
     * Returns true when the level is part of a parent/child hierarchy and has
     * an equivalent closed level.
     */
    boolean hasClosedPeer() {
        return levelReader instanceof ParentChildLevelReaderImpl;
    }


    interface LevelReader {
        /**
         * Adds constraints to a cell request for a member of this level.
         *
         * @param member
         * @param mapLevelToColumn
         * @param request
         * @return true if request is unsatisfiable (e.g. if the member is the
         *   null member)
         */
        boolean constrainRequest(
                RolapMember member,
                Map mapLevelToColumn,
                CellRequest request);
    }

    /**
     * Level reader for a regular level.
     */
    class RegularLevelReader implements LevelReader {
        public boolean constrainRequest(
                RolapMember member,
                Map mapLevelToColumn,
                CellRequest request) {
            assert member.getLevel() == RolapLevel.this;
            if (member.getKey() == null) {
                if (member == member.getHierarchy().getNullMember()) {
                    // cannot form a request if one of the members is null
                    return true;
                } else {
                    throw Util.newInternal("why is key null?");
                }
            }

            RolapStar.Column column =
                    (RolapStar.Column) mapLevelToColumn.get(RolapLevel.this);

            if (column == null) {
                // This hierarchy is not one which qualifies the starMeasure
                // (this happens in virtual cubes). The starMeasure only has
                // a value for the 'all' member of the hierarchy (or for the
                // default member if the hierarchy has no 'all' member)
                if (member == hierarchy.getDefaultMember() &&
                    !hierarchy.hasAll()) {
                    return false;
                }
                return true;
            }

            final MemberColumnConstraint constraint;
            if (member.isCalculated()) {
                constraint = null;
            } else {
                constraint = new MemberColumnConstraint(member);
            }

            // use the member as constraint, this will give us some
            //  optimization potential
            request.addConstrainedColumn(column, constraint);
            if (request.extendedContext &&
                    getNameExp() != null) {
                RolapStar.Column nameColumn = column.getNameColumn();

                Util.assertTrue(nameColumn != null);
                request.addConstrainedColumn(nameColumn, null);
            }

            if (member.isCalculated()) {
                return false;
            }

            // If member is unique without reference to its parent,
            // no further constraint is required.
            if (unique) {
                return false;
            }

            // Constrain the parent member, if any.
            RolapMember parent = (RolapMember) member.getParentMember();
            while (true) {
                if (parent == null) {
                    return false;
                }
                RolapLevel level = (RolapLevel) parent.getLevel();
                final LevelReader levelReader = level.levelReader;
                if (levelReader == this) {
                    // We are looking at a parent in a parent-child hierarchy,
                    // for example, we have moved from Fred to Fred's boss,
                    // Wilma. We don't want to include Wilma's key in the
                    // request.
                    parent = (RolapMember) parent.getParentMember();
                    continue;
                }
                return levelReader.constrainRequest(
                        parent, mapLevelToColumn, request);
            }
        }
    }

    /**
     * Level reader for a parent-child level which has a closed peer level.
     */
    class ParentChildLevelReaderImpl extends RegularLevelReader {
        /**
         * For a parent-child hierarchy with a closure provided by the schema,
         * the equivalent level in the closed hierarchy; otherwise null.
         */
        private final RolapLevel closedPeer;

        ParentChildLevelReaderImpl(RolapLevel closedPeer) {
            this.closedPeer = closedPeer;
        }

        public boolean constrainRequest(
                RolapMember member,
                Map mapLevelToColumn,
                CellRequest request) {

            // Replace a parent/child level by its closed equivalent, when
            // available; this is always valid, and improves performance by
            // enabling the database to compute aggregates.
            if (member.getDataMember() == null) {
                // Member has no data member because it IS the data
                // member of a parent-child hierarchy member. Leave
                // it be. We don't want to aggregate.
                return super.constrainRequest(
                        member, mapLevelToColumn, request);
            } else if (request.drillThrough) {
                member = (RolapMember) member.getDataMember();
                return super.constrainRequest(
                        member, mapLevelToColumn, request);
            } else {
                RolapLevel level = closedPeer;
                final RolapMember allMember = (RolapMember)
                        level.getHierarchy().getDefaultMember();
                assert allMember.isAll();
                member = new RolapMember(allMember, level,
                        ((RolapMember) member).getKey());
                return level.getLevelReader().constrainRequest(
                        member, mapLevelToColumn, request);
            }
        }
    }

    /**
     * Level reader for the level which contains the 'all' member.
     */
    class AllLevelReaderImpl implements LevelReader {
        public boolean constrainRequest(
                RolapMember member,
                Map mapLevelToColumn,
                CellRequest request) {
            // We don't need to apply any constraints.
            return false;
        }
    }

    /**
     * Level reader for the level which contains the null member.
     */
    class NullLevelReader implements LevelReader {
        public boolean constrainRequest(
                RolapMember member,
                Map mapLevelToColumn,
                CellRequest request) {
            return true;
        }
    }
}

// End RolapLevel.java
