/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.agg.*;
import mondrian.rolap.sql.SqlQuery;

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
        for (RolapLevel level : levels) {
            if (level.getName().equals(levelName)) {
                return level;
            }
        }
        return null;
    }

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
    private final SqlQuery.Datatype datatype;
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
            SqlQuery.Datatype datatype,
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
        this.approxRowCount = loadApproxRowCount(approxRowCount);
        this.flags = flags;
        final boolean isAll = (flags & ALL) == ALL;
        this.unique = (flags & UNIQUE) == UNIQUE;
        this.datatype = datatype;
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
        for (RolapProperty property : properties) {
            if (property.getExp() instanceof MondrianDef.Column) {
                checkColumn((MondrianDef.Column) property.getExp());
            }
        }
        this.properties = properties;
        List<Property> list = new ArrayList<Property>();
        for (Level level = this; level != null;
                level = level.getParentLevel()) {
            final Property[] levelProperties = level.getProperties();
            for (final Property levelProperty : levelProperties) {
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
        this.inheritedProperties = list.toArray(new RolapProperty[list.size()]);

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


    public RolapHierarchy getHierarchy() {
        return (RolapHierarchy) hierarchy;
    }

    private int loadApproxRowCount(String approxRowCount) {
        boolean notNullAndNumeric = approxRowCount != null && approxRowCount.matches("^\\d+$");
        if(notNullAndNumeric){

              return Integer.parseInt(approxRowCount);
        } else {
            // if approxRowCount is not set, return MIN_VALUE to indicate
            return Integer.MIN_VALUE;
        }
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

    /**
     * Return SQL expression for table column expression using the table 
     * alias provided.
     * @param sqlQuery sqlQuery context to generate SQL for
     * @param levelToColumnMap maps level to table columns
     * @param expr expression that references this level
     * @return SQL string for the expression
     */
    public String getExpressionWithAlias(
        SqlQuery sqlQuery,        
        Map<RolapLevel, RolapStar.Column> levelToColumnMap,
        MondrianDef.Expression expr) 
    {
        if (expr instanceof MondrianDef.Column &&
            levelToColumnMap != null) {
            RolapStar.Column targetColumn = levelToColumnMap.get(this);
            
            if (targetColumn != null) {
                String tableAlias = targetColumn.getTable().getAlias();
                
                if (tableAlias != null) {
                    MondrianDef.Column col = 
                        new MondrianDef.Column(
                            tableAlias, 
                            ((MondrianDef.Column)expr).getColumnName());
                    return col.getExpression(sqlQuery);
                }
            }
        }
       
        // If not column expression, or no way to map level to columns
        // return the default SQL translation for this expression.
        return expr.getExpression(sqlQuery);
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

    SqlQuery.Datatype getDatatype() {
        return datatype;
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

    // RME: this has to be public for two of the DrillThroughTest test.
    public
    MondrianDef.Expression getNameExp() {
        return nameExp;
    }

    private Property lookupProperty(List<Property> list, String propertyName) {
        for (Property property : list) {
            if (property.getName().equals(propertyName)) {
                return property;
            }
        }
        return null;
    }

    RolapLevel(RolapHierarchy hierarchy, int depth, MondrianDef.Level xmlLevel) {
        this(
            hierarchy, depth, xmlLevel.name, xmlLevel.getKeyExp(),
            xmlLevel.getNameExp(), xmlLevel.getCaptionExp(), xmlLevel.getOrdinalExp(),
            xmlLevel.getParentExp(), xmlLevel.nullParentValue,
            xmlLevel.closure, createProperties(xmlLevel),
            (xmlLevel.uniqueMembers ? UNIQUE : 0),
            xmlLevel.getDatatype(),
            HideMemberCondition.valueOf(xmlLevel.hideMemberIf),
            LevelType.valueOf(xmlLevel.levelType), xmlLevel.approxRowCount);

        if (!Util.isEmpty(xmlLevel.caption)) {
            setCaption(xmlLevel.caption);
        }
        if (!Util.isEmpty(xmlLevel.formatter)) {
            // there is a special member formatter class
            try {
                Class<MemberFormatter> clazz =
                    (Class<MemberFormatter>) Class.forName(xmlLevel.formatter);
                Constructor<MemberFormatter> ctor = clazz.getConstructor();
                memberFormatter = ctor.newInstance();
            } catch (Exception e) {
                throw MondrianResource.instance().MemberFormatterLoadFailed.ex(
                    xmlLevel.formatter, getUniqueName(), e);
            }
        }
    }

    // helper for constructor
    private static RolapProperty[] createProperties(
            MondrianDef.Level xmlLevel) {
        List<RolapProperty> list = new ArrayList<RolapProperty>();
        final MondrianDef.Expression nameExp = xmlLevel.getNameExp();

        if (nameExp != null) {
            list.add(new RolapProperty(
                    Property.NAME.name, Property.Datatype.TYPE_STRING,
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
        return list.toArray(new RolapProperty[list.size()]);
    }

    private static Property.Datatype convertPropertyTypeNameToCode(String type) {
        if (type.equals("String")) {
            return Property.Datatype.TYPE_STRING;
        } else if (type.equals("Numeric")) {
            return Property.Datatype.TYPE_NUMERIC;
        } else if (type.equals("Boolean")) {
            return Property.Datatype.TYPE_BOOLEAN;
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

    public RolapProperty[] getProperties() {
        return properties;
    }

    public Property[] getInheritedProperties() {
        return inheritedProperties;
    }

    public int getApproxRowCount() {
        return approxRowCount;
    }

    /**
     * Conditions under which a level's members may be hidden (thereby creating
     * a <dfn>ragged hierarchy</dfn>).
     */
    public enum HideMemberCondition {
        /** A member always appears. */
        Never,

        /** A member doesn't appear if its name is null or empty. */
        IfBlankName,

        /** A member appears unless its name matches its parent's. */
        IfParentsName
    }

    public OlapElement lookupChild(SchemaReader schemaReader, String name) {
        return lookupChild(schemaReader, name, MatchType.EXACT);
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, String name, MatchType matchType)
    {
        Member[] levelMembers = schemaReader.getLevelMembers(this, true);
        if (levelMembers.length > 0) {
            Member parent = levelMembers[0].getParentMember();
            return
                RolapUtil.findBestMemberMatch(
                    Arrays.asList(levelMembers),
                    (RolapMember) parent,
                    this,
                    name,
                    matchType,
                    false);
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
         * @param member Member to be constrained
         * @param levelToColumnMap
          *@param request Request to be constrained @return true if request is
         *   unsatisfiable (e.g. if the member is the null member)
         */
        boolean constrainRequest(
            RolapMember member,
            Map<RolapLevel, RolapStar.Column> levelToColumnMap,
            CellRequest request);

        /**
         * Adds constraints to a cache region for a member of this level.
         *
         * @param predicate Predicate
         * @param levelToColumnMap
         * @param cacheRegion Cache region to be constrained
         */
        void constrainRegion(
            StarColumnPredicate predicate,
            Map<RolapLevel, RolapStar.Column> levelToColumnMap,
            RolapCacheRegion cacheRegion);
    }

    /**
     * Level reader for a regular level.
     */
    class RegularLevelReader implements LevelReader {
        public boolean constrainRequest(
                RolapMember member,
                Map<RolapLevel, RolapStar.Column> levelToColumnMap,
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

            RolapStar.Column column = levelToColumnMap.get(RolapLevel.this);
            if (column == null) {
                // This hierarchy is not one which qualifies the starMeasure
                // (this happens in virtual cubes). The starMeasure only has
                // a value for the 'all' member of the hierarchy (or for the
                // default member if the hierarchy has no 'all' member)
                return member != hierarchy.getDefaultMember() ||
                    hierarchy.hasAll();
            }

            final StarColumnPredicate predicate;
            if (member.isCalculated()) {
                predicate = null;
            } else {
                predicate = false ? new MemberColumnPredicate(column, member) :
                    new ValueColumnPredicate(column, member.getSqlKey());
            }

            // use the member as constraint, this will give us some
            //  optimization potential
            request.addConstrainedColumn(column, predicate);
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
            RolapMember parent = member.getParentMember();
            while (true) {
                if (parent == null) {
                    return false;
                }
                RolapLevel level = parent.getLevel();
                final LevelReader levelReader = level.levelReader;
                if (levelReader == this) {
                    // We are looking at a parent in a parent-child hierarchy,
                    // for example, we have moved from Fred to Fred's boss,
                    // Wilma. We don't want to include Wilma's key in the
                    // request.
                    parent = parent.getParentMember();
                    continue;
                }
                return levelReader.constrainRequest(
                        parent, levelToColumnMap, request);
            }
        }

        public void constrainRegion(
            StarColumnPredicate predicate,
            Map<RolapLevel, RolapStar.Column> levelToColumnMap,
            RolapCacheRegion cacheRegion)
        {
            RolapStar.Column column = levelToColumnMap.get(RolapLevel.this);
            if (column == null) {
                // This hierarchy is not one which qualifies the starMeasure
                // (this happens in virtual cubes). The starMeasure only has
                // a value for the 'all' member of the hierarchy (or for the
                // default member if the hierarchy has no 'all' member)
                return;
            }

            if (predicate instanceof MemberColumnPredicate) {
                MemberColumnPredicate memberColumnPredicate =
                    (MemberColumnPredicate) predicate;
                RolapMember member = memberColumnPredicate.getMember();
                assert member.getLevel() == RolapLevel.this;
                assert !member.isCalculated();
                assert memberColumnPredicate.getMember().getKey() != null;
                assert !member.isNull();

                MemberTuplePredicate predicate2 =
                    new MemberTuplePredicate(
                        levelToColumnMap,
                        member);

                // use the member as constraint, this will give us some
                //  optimization potential
                cacheRegion.addPredicate(column, predicate);
                return;
            } else if (predicate instanceof RangeColumnPredicate) {
                RangeColumnPredicate rangeColumnPredicate =
                    (RangeColumnPredicate) predicate;
                final ValueColumnPredicate lowerBound =
                    rangeColumnPredicate.getLowerBound();
                RolapMember lowerMember;
                if (lowerBound == null) {
                    lowerMember = null;
                } else if (lowerBound instanceof MemberColumnPredicate) {
                    MemberColumnPredicate memberColumnPredicate =
                        (MemberColumnPredicate) lowerBound;
                    lowerMember = memberColumnPredicate.getMember();
                } else {
                    throw new UnsupportedOperationException();
                }
                final ValueColumnPredicate upperBound =
                    rangeColumnPredicate.getUpperBound();
                RolapMember upperMember;
                if (upperBound == null) {
                    upperMember = null;
                } else if (upperBound instanceof MemberColumnPredicate) {
                    MemberColumnPredicate memberColumnPredicate =
                        (MemberColumnPredicate) upperBound;
                    upperMember = memberColumnPredicate.getMember();
                } else {
                    throw new UnsupportedOperationException();
                }
                MemberTuplePredicate predicate2 =
                    new MemberTuplePredicate(
                        levelToColumnMap,
                        lowerMember,
                        !rangeColumnPredicate.getLowerInclusive(),
                        upperMember,
                        !rangeColumnPredicate.getUpperInclusive());
                // use the member as constraint, this will give us some
                //  optimization potential
                cacheRegion.addPredicate(predicate2);
                return;
            }

            // Unknown type of constraint.
            throw new UnsupportedOperationException();
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
                Map<RolapLevel, RolapStar.Column> levelToColumnMap,
                CellRequest request) {

            // Replace a parent/child level by its closed equivalent, when
            // available; this is always valid, and improves performance by
            // enabling the database to compute aggregates.
            if (member.getDataMember() == null) {
                // Member has no data member because it IS the data
                // member of a parent-child hierarchy member. Leave
                // it be. We don't want to aggregate.
                return super.constrainRequest(
                        member, levelToColumnMap, request);
            } else if (request.drillThrough) {
                member = (RolapMember) member.getDataMember();
                return super.constrainRequest(
                        member, levelToColumnMap, request);
            } else {
                RolapLevel level = closedPeer;
                final RolapMember allMember = (RolapMember)
                        level.getHierarchy().getDefaultMember();
                assert allMember.isAll();
                member = new RolapMember(allMember, level,
                        member.getKey());
                return level.getLevelReader().constrainRequest(
                        member, levelToColumnMap, request);
            }
        }

        public void constrainRegion(
            StarColumnPredicate predicate,
            Map<RolapLevel, RolapStar.Column> levelToColumnMap,
            RolapCacheRegion cacheRegion)
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Level reader for the level which contains the 'all' member.
     */
    class AllLevelReaderImpl implements LevelReader {
        public boolean constrainRequest(
                RolapMember member,
                Map<RolapLevel, RolapStar.Column> levelToColumnMap,
                CellRequest request) {
            // We don't need to apply any constraints.
            return false;
        }

        public void constrainRegion(
            StarColumnPredicate predicate,
            Map<RolapLevel, RolapStar.Column> levelToColumnMap,
            RolapCacheRegion cacheRegion)
        {
            // We don't need to apply any constraints.
        }
    }

    /**
     * Level reader for the level which contains the null member.
     */
    class NullLevelReader implements LevelReader {
        public boolean constrainRequest(
                RolapMember member,
                Map<RolapLevel, RolapStar.Column> levelToColumnMap,
                CellRequest request) {
            return true;
        }

        public void constrainRegion(
            StarColumnPredicate predicate,
            Map<RolapLevel, RolapStar.Column> levelToColumnMap,
            RolapCacheRegion cacheRegion)
        {
        }
    }
}

// End RolapLevel.java
