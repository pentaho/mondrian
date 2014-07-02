/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.TupleList;
import mondrian.olap.*;
import mondrian.olap.Member.MemberType;
import mondrian.resource.MondrianResource;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.*;
import mondrian.server.Locus;
import mondrian.server.monitor.SqlStatementEvent;
import mondrian.spi.Dialect;
import mondrian.util.*;

import org.eigenbase.util.property.StringProperty;

import java.sql.*;
import java.util.*;
import javax.sql.DataSource;

import static mondrian.rolap.LevelColumnLayout.OrderKeySource.*;

/**
 * A <code>SqlMemberSource</code> reads members from a SQL database.
 *
 * <p>It's a good idea to put a {@link CacheMemberReader} on top of this.
 *
 * @author jhyde
 * @since 21 December, 2001
 */
public class SqlMemberSource
    implements MemberReader, SqlTupleReader.MemberBuilder
{
    private final SqlConstraintFactory sqlConstraintFactory =
        SqlConstraintFactory.instance();
    protected final RolapCubeHierarchy hierarchy;
    private final DataSource dataSource;
    private MemberCache cache;
    private int lastOrdinal = 0;
    private final Map<Object, Object> valuePool;

    SqlMemberSource(RolapCubeHierarchy hierarchy) {
        this.hierarchy = hierarchy;
        this.dataSource =
            hierarchy.getRolapSchema().getInternalConnection().getDataSource();
        valuePool = ValuePoolFactoryFactory.getValuePoolFactory().create(this);
    }

    // implement MemberSource
    public RolapCubeHierarchy getHierarchy() {
        return hierarchy;
    }

    // implement MemberSource
    public boolean setCache(MemberCache cache) {
        this.cache = cache;
        return true; // yes, we support cache writeback
    }

    // implement MemberSource
    public int getMemberCount() {
        int count = 0;
        for (RolapCubeLevel level : hierarchy.getLevelList()) {
            count += getLevelMemberCount(level);
        }
        return count;
    }

    public RolapMember substitute(RolapMember member) {
        return member;
    }

    public RolapMember desubstitute(RolapMember member) {
        return member;
    }

    public RolapMember getMemberByKey(
        RolapCubeLevel level,
        List<Comparable> keyValues)
    {
        if (level.isAll()) {
            return null;
        }
        final List<RolapMember> list =
            getMembersInLevel(
                level,
                new MemberKeyConstraint(
                    level.attribute.getKeyList(),
                    keyValues));
        switch (list.size()) {
        case 0:
            return null;
        case 1:
            return list.get(0);
        default:
            throw Util.newError(
                "More than one member in level " + level + " with key "
                + keyValues);
        }
    }

    public RolapMember lookupMember(
        List<Id.Segment> uniqueNameParts,
        boolean failIfNotFound)
    {
        throw new UnsupportedOperationException();
    }

    public int getLevelMemberCount(RolapCubeLevel level) {
        if (level.isAll()) {
            return 1;
        }
        if (level.isMeasure()) {
            return level.getHierarchy().getMemberReader()
                .getMembersInLevel(level).size();
        }
        return getMemberCount(level, dataSource);
    }

    private int getMemberCount(RolapLevel level, DataSource dataSource) {
        boolean[] mustCount = new boolean[1];
        String sql = makeAttributeMemberCountSql(level.attribute, mustCount);
        final SqlStatement stmt =
            RolapUtil.executeQuery(
                dataSource,
                sql,
                new Locus(
                    Locus.peek().execution,
                    "SqlMemberSource.getLevelMemberCount",
                    "while counting members of level '" + level));
        try {
            ResultSet resultSet = stmt.getResultSet();
            int count;
            if (! mustCount[0]) {
                Util.assertTrue(resultSet.next());
                ++stmt.rowCount;
                count = resultSet.getInt(1);
            } else {
                // count distinct "manually"
                ResultSetMetaData rmd = resultSet.getMetaData();
                int nColumns = rmd.getColumnCount();
                String[] colStrings = new String[nColumns];
                count = 0;
                while (resultSet.next()) {
                    ++stmt.rowCount;
                    boolean isEqual = true;
                    for (int i = 0; i < nColumns; i++) {
                        String colStr = resultSet.getString(i + 1);
                        if (!Util.equals(colStr, colStrings[i])) {
                            isEqual = false;
                        }
                        colStrings[i] = colStr;
                    }
                    if (!isEqual) {
                        count++;
                    }
                }
            }
            return count;
        } catch (SQLException e) {
            throw stmt.handle(e);
        } finally {
            stmt.close();
        }
    }

    /**
     * Generates the SQL statement to count the members in
     * <code>attribute</code>. For example, <blockquote>
     *
     * <pre>SELECT count(*) FROM (
     *   SELECT DISTINCT "country", "state_province"
     *   FROM "customer") AS "init"</pre>
     *
     * </blockquote> counts the non-leaf "state_province" attribute (whose
     * key consists of the province name and the country). MySQL
     * doesn't allow SELECT-in-FROM, so we use the syntax<blockquote>
     *
     * <pre>SELECT count(DISTINCT "country", "state_province")
     * FROM "customer"</pre>
     *
     * </blockquote>. The leaf level requires a different query:<blockquote>
     *
     * <pre>SELECT count(*) FROM "customer"</pre>
     *
     * </blockquote> counts the leaf "name" level of the "customer" hierarchy.
     */
    private String makeAttributeMemberCountSql(
        RolapAttribute attribute,
        boolean[] mustCount)
    {
        mustCount[0] = false;
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder =
            new SqlTupleReader.ColumnLayoutBuilder();
        final Dialect dialect = getDialect();
        final SqlQueryBuilder queryBuilder =
            new SqlQueryBuilder(
                dialect,
                "while generating query to count members in attribute "
                + attribute,
                layoutBuilder);
        SqlQuery sqlQuery = queryBuilder.sqlQuery;
        final SqlQueryBuilder.Joiner joiner =
            SqlQueryBuilder.AutoJoiner.INSTANCE;
        if (!dialect.allowsFromQuery()) {
            List<String> columnList = new ArrayList<String>();
            int columnCount = 0;
            for (RolapSchema.PhysColumn column : attribute.getKeyList()) {
                if (columnCount > 0) {
                    if (dialect.allowsCompoundCountDistinct()) {
                        // no op.
                    } else if (true) {
                        // for databases where both SELECT-in-FROM and
                        // COUNT DISTINCT do not work, we do not
                        // generate any count and do the count
                        // distinct "manually".
                        mustCount[0] = true;
                    }
                }
                queryBuilder.addColumn(
                    queryBuilder.column(column, hierarchy.cubeDimension),
                    Clause.FROM,
                    joiner,
                    null);

                String keyExp = column.toSql();
                if (columnCount > 0
                    && !dialect.allowsCompoundCountDistinct()
                    && dialect.getDatabaseProduct()
                    == Dialect.DatabaseProduct.SYBASE)
                {
                    keyExp = "convert(varchar, " + columnList + ")";
                }
                columnList.add(keyExp);

                ++columnCount;
            }
            if (mustCount[0]) {
                for (String colDef : columnList) {
                    final String exp = dialect.generateCountExpression(colDef);
                    String alias = sqlQuery.addSelect(exp, null);
                    sqlQuery.addOrderBy(exp, alias, true, false, true, true);
                }
            } else {
                List<String> list = new ArrayList<String>();
                for (String colDef : columnList) {
                    list.add(dialect.generateCountExpression(colDef));
                }
                sqlQuery.addSelect(
                    "count(DISTINCT " + Util.commaList(list) + ")", null);
            }
            return queryBuilder.toSqlAndTypes().left;

        } else {
            sqlQuery.setDistinct(true);
            for (RolapSchema.PhysColumn column : attribute.getKeyList()) {
                queryBuilder.addColumn(
                    queryBuilder.column(column, hierarchy.cubeDimension),
                    Clause.SELECT);
            }
            SqlQuery outerQuery =
                SqlQuery.newQuery(
                    dialect,
                    "while generating query to count members in attribute "
                    + attribute);
            outerQuery.addSelect("count(*)", null);
            // Note: the "init" is for Postgres, which requires
            // FROM-queries to have an alias
            boolean failIfExists = true;
            queryBuilder.flush();
            outerQuery.addFrom(sqlQuery, "init", failIfExists);
            return outerQuery.toString();
        }
    }


    public List<RolapMember> getMembers() {
        return getMembers(dataSource);
    }

    private List<RolapMember> getMembers(DataSource dataSource) {
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder =
            new SqlTupleReader.ColumnLayoutBuilder();
        String sql =
            makeKeysSql(
                layoutBuilder,
                Util.last(hierarchy.levelList).attribute.getKeyList());
        List<SqlStatement.Type> types = layoutBuilder.types;
        SqlStatement stmt =
            RolapUtil.executeQuery(
                dataSource, sql, types, 0, 0,
                new SqlStatement.StatementLocus(
                    null,
                    "SqlMemberSource.getMembers",
                    "while building member cache",
                    SqlStatementEvent.Purpose.TUPLES, 0),
                -1, -1, null);
        final SqlTupleReader.ColumnLayout columnLayout =
            layoutBuilder.toLayout();
        try {
            final Map<Object, SqlStatement.Accessor> accessors =
                stmt.getAccessors();
            List<RolapMember> list = new ArrayList<RolapMember>();
            final Map<Object, RolapMember> map =
                new HashMap<Object, RolapMember>();
            RolapMember root = null;
            if (hierarchy.hasAll()) {
                root = hierarchy.getAllMember();
                list.add(root);
            }

            int limit = MondrianProperties.instance().ResultLimit.get();
            ResultSet resultSet = stmt.getResultSet();
            while (resultSet.next()) {
                ++stmt.rowCount;
                if (limit > 0 && limit < stmt.rowCount) {
                    // result limit exceeded, throw an exception
                    throw stmt.handle(
                        MondrianResource.instance().MemberFetchLimitExceeded
                            .ex(limit));
                }

                RolapMember member = root;
                for (RolapCubeLevel level : hierarchy.getLevelList()) {
                    if (level.isAll()) {
                        continue;
                    }
                    final LevelColumnLayout<Integer> levelLayout =
                        columnLayout.levelLayoutMap.get(level);
                    // TODO: pre-allocate these, one per level; remember to
                    // clone list (using Flat2List or Flat3List if appropriate)
                    final Comparable[] keyValues =
                        new Comparable[level.attribute.getKeyList().size()];

                    for (int i = 0; i < levelLayout.getKeys().size(); i++) {
                        int keyOrdinal = levelLayout.getKeys().get(i);
                        Comparable value = accessors.get(keyOrdinal).get();
                        keyValues[i] = toComparable(value);
                    }
                    RolapMember parent = member;

                    Comparable key = keyValues[0];
                    if (keyValues.length == 1) {
                        member = map.get(key);
                    } else {
                        member = map.get(Arrays.asList(keyValues));
                        if (member == null) {
                            key = RolapMember.Key.create(keyValues);
                        }
                    }
                    if (member == null) {
                        final Comparable captionValue;
                        if (levelLayout.getCaptionKey() >= 0) {
                            captionValue =
                                accessors.get(
                                    levelLayout.getCaptionKey()).get();
                        } else {
                            captionValue = null;
                        }
                        final String nameValue;
                        final Comparable nameObject;
                        if (levelLayout.getNameKey() >= 0) {
                            nameObject =
                                accessors.get(levelLayout.getNameKey()).get();
                            nameValue =
                                nameObject == null
                                    ? null
                                    : String.valueOf(nameObject);
                        } else {
                            nameObject = null;
                            nameValue = null;
                        }
                        Larders.LarderBuilder builder =
                            new Larders.LarderBuilder();
                        builder.add(Property.NAME, nameValue);
                        if (captionValue != null) {
                            String caption = captionValue.toString();
                            if (!caption.equals(nameValue)) {
                                builder.caption(caption);
                            }
                        }
                        RolapMemberBase memberBase =
                            new RolapMemberBase(
                                parent, level, key,
                                MemberType.REGULAR,
                                RolapMemberBase.deriveUniqueName(
                                    parent, level, nameValue, false),
                                builder.build());
                        memberBase.setOrdinal(lastOrdinal++);
                        member = memberBase;
                        list.add(member);
                        map.put(key, member);

                        // REVIEW jvs 20-Feb-2007:  What about caption? TODO:

                        order: {
                            Comparable orderKey;
                            switch (levelLayout.getOrderBySource()) {
                            case NONE:
                                break order;
                            case KEY:
                                orderKey = key;
                                break;
                            case NAME:
                                orderKey = nameObject;
                                break;
                            case MAPPED:
                                orderKey =
                                    getCompositeKey(
                                        accessors,
                                        levelLayout.getOrderByKeys());
                                break;
                            default:
                                throw
                                    Util.unexpected(
                                        levelLayout.getOrderBySource());
                            }
                            ((RolapMemberBase) member).setOrderKey(orderKey);
                        }
                    }

                    int i = 0;
                    for (Property property : level.attribute.getProperties()) {
                        int propertyOrdinal =
                            levelLayout.getPropertyKeys().get(i++);
                        // REVIEW emcdermid 9-Jul-2009:
                        // Should we also look up the value in the
                        // pool here, rather than setting it directly?
                        // Presumably the value is already in the pool
                        // as a result of makeMember().
                        member.setProperty(
                            property,
                            accessors.get(propertyOrdinal).get());
                    }
                }
            }

            return list;
        } catch (SQLException e) {
            throw stmt.handle(e);
        } finally {
            stmt.close();
        }
    }

    private Dialect getDialect() {
        return hierarchy.getDimension().getSchema().getDialect();
    }

    public static Comparable toComparable(Object value) {
        if (value == null) {
            return RolapUtil.sqlNullValue;
        } else if (value instanceof byte[]) {
            // Some drivers (e.g. Derby) return byte arrays for binary columns,
            // but byte arrays do not implement Comparable.
            return new String((byte[]) value);
        } else if (value instanceof Boolean) {
            // Canonize to save a bit of memory.
            return Boolean.valueOf((Boolean) value);
        } else {
            // All other known return values are comparable.
            return (Comparable) value;
        }
    }

    private String makeKeysSql(
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder,
        List<RolapSchema.PhysColumn> keyList)
    {
        SqlQuery sqlQuery =
            SqlQuery.newQuery(
                getDialect(),
                "while generating query to retrieve members of " + hierarchy);
        final SqlQueryBuilder queryBuilder =
            new SqlQueryBuilder(
                sqlQuery, layoutBuilder, keyList);
        final SqlQueryBuilder.Joiner joiner =
            SqlQueryBuilder.AutoJoiner.INSTANCE;
        final RolapCubeDimension dimension = hierarchy.cubeDimension;
        queryBuilder.addColumns(
            keyList, dimension, Clause.FROM, joiner);
        for (RolapCubeLevel level : hierarchy.getLevelList()) {
            queryBuilder.addColumns(
                level.getOrderByList(), dimension, Clause.SELECT_ORDER, joiner);
            queryBuilder.addColumns(
                level.attribute.getKeyList(), dimension, Clause.SELECT_GROUP,
                joiner);
            for (RolapProperty property
                : level.attribute.getExplicitProperties())
            {
                // Some dialects allow us to eliminate properties from
                // the group by that are functionally dependent on the
                // level value.
                queryBuilder.addColumns(
                    property.attribute.getKeyList(), dimension,
                    Clause.SELECT.maybeGroup(
                        !sqlQuery.getDialect().allowsSelectNotInGroupBy()),
                    joiner);
            }
        }
        return sqlQuery.toSql();
    }

    // implement MemberReader
    public List<RolapMember> getMembersInLevel(
        RolapCubeLevel level)
    {
        TupleConstraint constraint =
            sqlConstraintFactory.getLevelMembersConstraint(null);
        return getMembersInLevel(level, constraint);
    }

    public List<RolapMember> getMembersInLevel(
        RolapCubeLevel level,
        TupleConstraint constraint)
    {
        if (level.isAll()) {
            return Collections.singletonList(hierarchy.getAllMember());
        }
        final TupleReader tupleReader = new SqlTupleReader(constraint);
        tupleReader.addLevelMembers(level, this, null);
        final TupleList tupleList =
            tupleReader.readMembers(
                hierarchy.getDimension().getSchema().getDialect(),
                dataSource,
                null,
                null);

        assert tupleList.getArity() == 1;
        return Util.cast(tupleList.slice(0));
    }

    public MemberCache getMemberCache() {
        return cache;
    }

    public Object getMemberCacheLock() {
        return cache;
    }

    // implement MemberSource
    public List<RolapMember> getRootMembers() {
        return getMembersInLevel(hierarchy.getLevelList().get(0));
    }

    /**
     * Generates the SQL statement to access the children of
     * <code>member</code>. For example, <blockquote>
     *
     * <pre>SELECT "city"
     * FROM "customer"
     * WHERE "country" = 'USA'
     * AND "state_province" = 'BC'
     * GROUP BY "city"</pre>
     * </blockquote> retrieves the children of the member
     * <code>[Canada].[BC]</code>.
     *
     * <p>Note that this method is never called in the context of
     * virtual cubes, it is only called on regular cubes.
     *
     * <p>See also {@link SqlTupleReader#makeLevelMembersSql}.
     *
     * @return Query, or null if query will never return any rows
     */
    String makeChildMemberSql(
        RolapMember member,
        final MemberChildrenConstraint constraint,
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder)
    {
        Util.deprecated(
            "make caption, key, name etc. properties of a level so can handle in a loop",
            false);
        Util.deprecated("remove commented code in this method", false);
        // If this is a non-empty constraint, it is more efficient to join to
        // an aggregate table than to the fact table. See whether a suitable
        // aggregate table exists.
        final RolapMeasureGroup aggMeasureGroup =
            chooseAggStar(constraint, member);
        //final AggStar aggStar1 = (AggStar) aggStar; // FIXME;

        // Create the condition, which is either the parent member or
        // the full context (non empty).
        final RolapStarSet starSet = constraint.createStarSet(aggMeasureGroup);
        final Dialect dialect = getDialect();
        final SqlQueryBuilder queryBuilder =
            new SqlQueryBuilder(
                dialect,
                "while generating query to retrieve children of member "
                + member,
                layoutBuilder);
        SqlQuery sqlQuery = queryBuilder.sqlQuery;
        constraint.addMemberConstraint(queryBuilder, starSet, member);

        RolapCubeLevel level = member.getLevel().getChildLevel();
/*
        boolean levelCollapsed =
            (aggStar != null)
            && isLevelCollapsed(
                aggStar1,
                (RolapCubeLevel) level,
                starSet.getMeasureGroup());
*/
        layoutBuilder.createLayoutFor(level);

        // If constraint is 'anchored' to a fact table, add join conditions to
        // the fact table (via the table containing the dimension's key, if
        // the dimension is a snowflake). Otherwise just add the path from the
        // dimension's key.
        //
        // TODO: always joining to dimension key table will automatically
        // filter out childless snowflake members.
        queryBuilder.joinToDimensionKey = true;
        Util.Function1<RolapSchema.PhysColumn, RolapSchema.PhysColumn> fn =
            Util.identityFunctor();
        if (starSet.getMeasureGroup() != null) {
            final RolapMeasureGroup measureGroup;
            if (aggMeasureGroup != null) {
                measureGroup = aggMeasureGroup;
                fn = new Util.Function1<
                    RolapSchema.PhysColumn, RolapSchema.PhysColumn>()
                    {
                    public RolapSchema.PhysColumn apply(
                        RolapSchema.PhysColumn param)
                    {
                        for (Pair<RolapStar.Column, RolapSchema.PhysColumn> pair
                            : aggMeasureGroup.copyColumnList)
                        {
                            if (pair.right.equals(param)) {
                                return pair.left.getExpression();
                            }
                        }
                        return param;
                    }
                };
            } else {
                measureGroup = starSet.getMeasureGroup();
            }
            queryBuilder.fact = measureGroup;
            if (Util.deprecated(false, false)) {
                final RolapSchema.PhysPath path =
                    measureGroup.getPath(level.getDimension());
                if (path != null) {
                    // path is null for CopyLink
                    for (RolapSchema.PhysHop hop : path.hopList) {
                        sqlQuery.addFrom(hop.relation, null, false);
                        if (hop.link != null) {
                            // first hop has no link (because no
                            // predecessor to join to)
                            sqlQuery.addWhere(hop.link.sql);
                        }
                    }
                }
            }
        }

        // Add lower tables to the FROM clause. This filters out children
        // that have no ancestors in lower snowflake tables. This situation
        // is a breach of referential integrity, and it is not strictly
        // necessary that we make the effort to filter out such members, but
        // we did this up to 3.1 (before PhysicalSchema was introduced) so
        // we continue to do so for backwards compatibility.
//        for (RolapLevel descendantLevel = level.getChildLevel();
//             descendantLevel != null;
//             descendantLevel = descendantLevel.getChildLevel())
//        {
//            descendantLevel.getKeyPath().addToFrom(sqlQuery, false);
//        }

        // in non empty mode the level table must be joined to the fact
        // table
        constraint.addLevelConstraint(sqlQuery, starSet, level);

/*
        if (levelCollapsed) {
            // if this is a collapsed level, add a join between key and aggstar;
            // also may need to join parent levels to make selection unique
            final RolapCubeLevel cubeLevel = (RolapCubeLevel) level;
            final RolapMeasureGroup measureGroup = starSet.getMeasureGroup();
            for (RolapSchema.PhysColumn column : level.attribute.getKeyList()) {
                hierarchy.addToFromInverse(sqlQuery, column);
                RolapStar.Column starColumn =
                    measureGroup.getRolapStarColumn(
                        cubeLevel.cubeDimension,
                        column,
                        false);
                int bitPos = starColumn.getBitPosition();
                AggStar.Table.Column aggColumn = aggStar1.lookupColumn(bitPos);
                sqlQuery.addWhere(
                    aggColumn.getExpression().toSql() + " = " + column.toSql());
            }
        }
*/

        if (sqlQuery.isUnsatisfiable()) {
            return null;
        }
        queryBuilder.addColumns(
            Util.transform(fn, member.getLevel().attribute.getKeyList()),
            member.getDimension(),
            Clause.FROM,
            SqlQueryBuilder.NullJoiner.INSTANCE,
            false);


        return projectProperties(
            layoutBuilder, queryBuilder, level,
            level.attribute.getProperties(), fn);
    }

    private String projectProperties(
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder,
        SqlQueryBuilder queryBuilder,
        RolapCubeLevel level,
        List<RolapProperty> properties,
        Util.Function1<RolapSchema.PhysColumn, RolapSchema.PhysColumn> fn)
    {
        final SqlQueryBuilder.Joiner joiner =
            SqlQueryBuilder.NullJoiner.INSTANCE;
        final SqlTupleReader.LevelLayoutBuilder levelLayout =
            layoutBuilder.createLayoutFor(level);

        final RolapCubeDimension dimension = level.cubeDimension;
        for (RolapSchema.PhysColumn key : level.getOrderByList()) {
            levelLayout.orderByOrdinalList.add(
                queryBuilder.addColumn(
                    queryBuilder.column(fn.apply(key), dimension),
                    Clause.SELECT_GROUP_ORDER, joiner, null, false));
        }

        for (RolapSchema.PhysColumn column : level.attribute.getKeyList()) {
            // REVIEW: also need to join each attr to dim key?
            levelLayout.keyOrdinalList.add(
                queryBuilder.addColumn(
                    queryBuilder.column(fn.apply(column), dimension),
                    Clause.SELECT_GROUP, joiner, null, false));
        }

        if (level.attribute.getNameExp() != null) {
            final RolapSchema.PhysColumn exp =
                fn.apply(level.attribute.getNameExp());
            levelLayout.nameOrdinal =
                queryBuilder.addColumn(
                    queryBuilder.column(exp, dimension),
                    Clause.SELECT_GROUP, joiner, null);
        }

        if (level.attribute.getCaptionExp() != null) {
            final RolapSchema.PhysColumn exp =
                fn.apply(level.attribute.getCaptionExp());
            levelLayout.captionOrdinal =
                queryBuilder.addColumn(
                    queryBuilder.column(exp, dimension),
                    Clause.SELECT_GROUP, joiner, null);
        }

        for (RolapProperty property : properties) {
            // TODO: properties that are composite, or have key != name exp
            final RolapSchema.PhysColumn exp =
                fn.apply(property.attribute.getNameExp());

            // Some dialects allow us to eliminate properties from the
            // group by that are functionally dependent on the level value
            final Clause clause;
            if (!queryBuilder.sqlQuery.getDialect().allowsSelectNotInGroupBy()
                || !property.dependsOnLevelValue())
            {
                clause = Clause.SELECT_GROUP;
            } else {
                clause = Clause.SELECT;
            }
            levelLayout.propertyOrdinalList.add(
                queryBuilder.addColumn(
                    queryBuilder.column(exp, dimension), clause, joiner, null));
        }

        final Pair<String, List<SqlStatement.Type>> pair =
            queryBuilder.toSqlAndTypes();
        layoutBuilder.types.addAll(pair.right);
        return pair.left;
    }

    private static AggStar chooseAggStar0(
        MemberChildrenConstraint constraint,
        RolapMember member)
    {
        Util.deprecated("method not used; remove", true);

        if (!MondrianProperties.instance().UseAggregates.get()
            || !(constraint instanceof SqlContextConstraint))
        {
            return null;
        }

        SqlContextConstraint contextConstraint =
                (SqlContextConstraint) constraint;
        Evaluator evaluator = contextConstraint.getEvaluator();
        RolapCube cube = (RolapCube) evaluator.getCube();
        RolapStar star = cube.getStar();
        final int starColumnCount = star.getColumnCount();
        BitKey measureBitKey = BitKey.Factory.makeBitKey(starColumnCount);
        BitKey levelBitKey = BitKey.Factory.makeBitKey(starColumnCount);

        // Convert global ordinal to cube based ordinal (the 0th dimension
        // is always [Measures])
        final Member[] members = evaluator.getNonAllMembers();

        // if measure is calculated, we can't continue
        if (!(members[0] instanceof RolapBaseCubeMeasure)) {
            return null;
        }
        RolapBaseCubeMeasure measure = (RolapBaseCubeMeasure)members[0];
        // we need to do more than this!  we need the rolap star ordinal, not
        // the rolap cube

        int bitPosition = measure.getStarMeasure().getBitPosition();
        int ordinal = measure.getOrdinal();

        // childLevel will always end up being a RolapCubeLevel, but the API
        // calls into this method can be both shared RolapMembers and
        // RolapCubeMembers so this cast is necessary for now. Also note that
        // this method will never be called in the context of a virtual cube
        // so baseCube isn't necessary for retrieving the correct column

        // set a bit for each level which is constrained in the context
        final CellRequest request =
            RolapAggregationManager.makeRequest(members);
        if (request == null) {
            // One or more calculated members. Cannot use agg table.
            return null;
        }
        // TODO: RME why is this using the array of constrained columns
        // from the CellRequest rather than just the constrained columns
        // BitKey (method getConstrainedColumnsBitKey)?
        RolapStar.Column[] columns = request.getConstrainedColumns();
        for (RolapStar.Column column1 : columns) {
            levelBitKey.set(column1.getBitPosition());
        }

        // set the masks
        levelBitKey.set(bitPosition);
        measureBitKey.set(ordinal);

        // find the aggstar using the masks
        return AggregationManager.findAgg(
            star, levelBitKey, measureBitKey, new boolean[]{false});
    }

    private static RolapMeasureGroup chooseAggStar(
        MemberChildrenConstraint constraint,
        final RolapMember member)
    {
        if (!MondrianProperties.instance().UseAggregates.get()) {
            return null;
        }
        if (!(constraint instanceof SqlContextConstraint)) {
            return null;
        }
        final SqlContextConstraint contextConstraint =
            (SqlContextConstraint) constraint;
        final Evaluator evaluator = contextConstraint.getEvaluator();
        final RolapMeasureGroup measureGroup = evaluator.getMeasureGroup();
        if (measureGroup == null) {
            // measure is calculated; we can't continue
            return null;
        }
        RolapStar star = measureGroup.getStar();
        final int starColumnCount = star.getColumnCount();
        BitKey measureBitKey = BitKey.Factory.makeBitKey(starColumnCount);
        BitKey levelBitKey = BitKey.Factory.makeBitKey(starColumnCount);

        // Convert global ordinal to cube based ordinal (the 0th dimension
        // is always [Measures])
        final Member[] members = evaluator.getNonAllMembers();

        RolapStoredMeasure measure = (RolapStoredMeasure)members[0];
        // we need to do more than this!  we need the rolap star ordinal, not
        // the rolap cube

        final RolapCubeLevel level = member.getLevel().getChildLevel();
        for (RolapSchema.PhysColumn column : level.attribute.getKeyList()) {
            RolapStar.Column starColumn =
                measureGroup.getRolapStarColumn(
                    level.cubeDimension, column, false);
            levelBitKey.set(starColumn.getBitPosition());
        }
        int ordinal = measure.getOrdinal();

        // set a bit for each level which is constrained in the context
        final CellRequest request =
            RolapAggregationManager.makeRequest(members);
        if (request == null) {
            // One or more calculated members. Cannot use agg table.
            return null;
        }

        // TODO: RME why is this using the array of constrained columns
        // from the CellRequest rather than just the constrained columns
        // BitKey (method getConstrainedColumnsBitKey)?
        Util.deprecated("try using getConstrainedColumnsBitKey", false);
        RolapStar.Column[] columns = request.getConstrainedColumns();
        for (RolapStar.Column column1 : columns) {
            levelBitKey.set(column1.getBitPosition());
        }

        // set the masks
//        levelBitKey.set(bitPosition);
        measureBitKey.set(ordinal);

        // find the aggregate star using the masks
        return ((RolapCube) evaluator.getCube()).galaxy.findAgg(
            star, levelBitKey, measureBitKey, new boolean[]{false});
    }

    /**
     * Determines whether the given aggregate table has the dimension level
     * specified within in (AggStar.FactTable) it, aka collapsed,
     * or associated with foreign keys (AggStar.DimTable)
     *
     * @param aggStar aggregate star if exists
     * @param level Level
     * @param measureGroup Measure group
     * @return Whether agg table has level
     */
    public static boolean isLevelCollapsed(
        AggStar aggStar,
        RolapCubeLevel level,
        RolapMeasureGroup measureGroup)
    {
        if (level.isAll()) {
            return false;
        }
        final RolapStar.Column starColumn =
            level.getBaseStarKeyColumn(measureGroup);
        int bitPos = starColumn.getBitPosition();
        AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
        return aggColumn.getTable() instanceof AggStar.FactTable;
    }

    public void getMemberChildren(
        List<RolapMember> parentMembers,
        List<RolapMember> children)
    {
        MemberChildrenConstraint constraint =
            sqlConstraintFactory.getMemberChildrenConstraint(null);
        getMemberChildren(parentMembers, children, constraint);
    }

    public void getMemberChildren(
        List<RolapMember> parentMembers,
        List<RolapMember> children,
        MemberChildrenConstraint mcc)
    {
        // try to fetch all children at once
        RolapCubeLevel childLevel =
            getCommonChildLevelForDescendants(parentMembers);
        if (childLevel != null) {
            TupleConstraint lmc =
                sqlConstraintFactory.getDescendantsConstraint(
                    parentMembers, mcc);
            List<RolapMember> list =
                getMembersInLevel(childLevel, lmc);
            children.addAll(list);
            return;
        }

        // fetch them one by one
        for (RolapMember parentMember : parentMembers) {
            getMemberChildren(parentMember, children, mcc);
        }
    }

    public void getMemberChildren(
        RolapMember parentMember,
        List<RolapMember> children)
    {
        MemberChildrenConstraint constraint =
            sqlConstraintFactory.getMemberChildrenConstraint(null);
        getMemberChildren(parentMember, children, constraint);
    }

    public void getMemberChildren(
        RolapMember parentMember,
        List<RolapMember> children,
        MemberChildrenConstraint constraint)
    {
        // allow parent child calculated members through
        // this fixes the non closure parent child hierarchy bug
        if (!parentMember.isAll()
            && parentMember.isCalculated()
            && !parentMember.getLevel().isParentChild())
        {
            return;
        }
        getMemberChildren2(parentMember, children, constraint);
    }

    /**
     * If all parents belong to the same level and no parent/child is involved,
     * returns that level; this indicates that all member children can be
     * fetched at once. Otherwise returns null.
     */
    private RolapCubeLevel getCommonChildLevelForDescendants(
        List<RolapMember> parents)
    {
        // at least two members required
        if (parents.size() < 2) {
            return null;
        }
        RolapCubeLevel parentLevel = null;
        RolapCubeLevel childLevel = null;
        for (RolapMember member : parents) {
            // we can not fetch children of calc members
            if (member.isCalculated()) {
                return null;
            }
            // first round?
            if (parentLevel == null) {
                parentLevel = member.getLevel();
                // check for parent/child
                if (parentLevel.isParentChild()) {
                    return null;
                }
                childLevel = parentLevel.getChildLevel();
                if (childLevel == null) {
                    return null;
                }
                if (childLevel.isParentChild()) {
                    return null;
                }
            } else if (parentLevel != member.getLevel()) {
                return null;
            }
        }
        return childLevel;
    }

    private void getMemberChildren2(
        final RolapMember parentMember,
        List<RolapMember> children,
        MemberChildrenConstraint constraint)
    {
        final String sql;
        boolean parentChild;
        final RolapCubeLevel parentLevel = parentMember.getLevel();
        final RolapCubeLevel childLevel;
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder =
            new SqlTupleReader.ColumnLayoutBuilder();
        if (parentLevel.isParentChild()) {
            sql = makeChildMemberSqlPC(parentMember, layoutBuilder);
            parentChild = true;
            childLevel = parentLevel;
        } else {
            childLevel = parentLevel.getChildLevel();
            if (childLevel == null) {
                // member is at last level, so can have no children
                return;
            }
            parentChild = childLevel.isParentChild();
            if (parentChild) {
                sql = makeChildMemberSql_PCRoot(parentMember, layoutBuilder);
            } else {
                sql = makeChildMemberSql(
                    parentMember, constraint, layoutBuilder);
            }
        }
        if (sql == null) {
            return;
        }
        final List<SqlStatement.Type> types = layoutBuilder.types;
        SqlStatement stmt =
            RolapUtil.executeQuery(
                dataSource, sql, types, 0, 0,
                new SqlStatement.StatementLocus(
                    Locus.peek().execution,
                    "SqlMemberSource.getMemberChildren",
                    "while building member cache",
                    SqlStatementEvent.Purpose.TUPLES, 0),
                -1, -1, null);
        try {
            int limit = MondrianProperties.instance().ResultLimit.get();

            final Map<Object, SqlStatement.Accessor> accessors =
                stmt.getAccessors();
            ResultSet resultSet = stmt.getResultSet();
            final SqlTupleReader.ColumnLayout fullLayout =
                layoutBuilder.toLayout();
            final LevelColumnLayout<Integer> layout =
                parentChild
                && !parentMember.isAll()
                && childLevel.getParentAttribute() != null
                && childLevel.getClosure() != null
                    ? fullLayout.levelLayoutMap.get(
                        childLevel.getClosure().closedPeerLevel.getChildLevel())
                    : fullLayout.levelLayoutMap.get(
                        childLevel);
            assert layout != null
                    : "Error!!";
            while (resultSet.next()) {
                ++stmt.rowCount;
                if (limit > 0 && limit < stmt.rowCount) {
                    // result limit exceeded, throw an exception
                    throw MondrianResource.instance().MemberFetchLimitExceeded
                        .ex(limit);
                }

                final Comparable[] keyValues =
                    new Comparable[layout.getKeys().size()];
                for (int i = 0; i < layout.getKeys().size(); i++) {
                    Comparable value =
                        accessors.get(layout.getKeys().get(i)).get();
                    keyValues[i] = toComparable(value);
                }
                RolapMember member =
                    cache.getMember(
                        childLevel,
                        RolapMember.Key.quick(keyValues));
                if (member == null) {
                    final Comparable keyClone =
                        RolapMember.Key.create(keyValues);
                    final Comparable captionValue;
                    if (layout.getCaptionKey() >= 0) {
                        captionValue =
                            accessors.get(layout.getCaptionKey()).get();
                    } else {
                        captionValue = null;
                    }
                    final Comparable nameObject;
                    final String nameValue;
                    if (layout.getNameKey() >= 0) {
                        nameObject = accessors.get(layout.getNameKey()).get();
                        nameValue =
                            nameObject == null
                                ? RolapUtil.mdxNullLiteral()
                                : String.valueOf(nameObject);
                    } else {
                        nameObject = null;
                        nameValue = null;
                    }
                    final Comparable orderKey;
                    switch (layout.getOrderBySource()) {
                    case NONE:
                        orderKey = null;
                        break;
                    case KEY:
                        orderKey = keyClone;
                        break;
                    case NAME:
                        orderKey = nameObject;
                        break;
                    case MAPPED:
                        orderKey =
                            getCompositeKey(accessors, layout.getOrderByKeys());
                        break;
                    default:
                        throw Util.unexpected(layout.getOrderBySource());
                    }
                    member =
                        makeMember(
                            parentMember, childLevel, keyClone, captionValue,
                            nameValue, orderKey, parentChild, stmt, layout);
                }
                children.add(member);
            }
        } catch (SQLException e) {
            throw stmt.handle(e);
        } finally {
            stmt.close();
        }
    }

    public RolapMember makeMember(
        RolapMember parentMember,
        RolapCubeLevel childLevel,
        Comparable key,
        Object captionValue,
        String nameValue,
        Comparable orderKey,
        boolean parentChild,
        DBStatement stmt,
        LevelColumnLayout layout)
        throws SQLException
    {
        final Larders.LarderBuilder builder = new Larders.LarderBuilder();
        builder.add(Property.NAME, nameValue);

        if (captionValue != null) {
            final String caption = captionValue.toString();
            if (!caption.equals(nameValue)) {
                builder.caption(caption);
            }
        }
        RolapMemberBase member =
            new RolapMemberBase(
                parentMember,
                childLevel,
                key,
                MemberType.REGULAR,
                RolapMemberBase.deriveUniqueName(
                    parentMember, childLevel, nameValue, false),
                builder.build());
        assert parentMember == null
            || parentMember.getLevel().getDepth() == childLevel.getDepth() - 1
            || childLevel.isParentChild();
        setOrderKey(orderKey, layout, member);
        if (parentChild) {
            // Create a 'public' and a 'data' member. The public member is
            // calculated, and its value is the aggregation of the data member
            // and all of the children. The children and the data member belong
            // to the parent member; the data member does not have any
            // children.
            member =
                childLevel.hasClosedPeer()
                    ? new RolapParentChildMember(
                        parentMember, childLevel, key, member)
                    : new RolapParentChildMemberNoClosure(
                        parentMember, childLevel, key, member);
            setOrderKey(orderKey, layout, member);
        }
        final Map<Object, SqlStatement.Accessor> accessors =
            stmt.getAccessors();
        if (layout.getNameKey()
            != layout.getKeys().get(layout.getKeys().size() - 1)
            && false)
        {
            Comparable name = accessors.get(layout.getNameKey()).get();
            member.setProperty(
                Property.NAME,
                name == null
                    ? RolapUtil.sqlNullValue.toString()
                    : name.toString());
        }
        int j = 0;
        for (RolapProperty property
            : childLevel.attribute.getExplicitProperties())
        {
            member.setProperty(
                property,
                getPooledValue(
                    accessors.get(layout.getPropertyKeys().get(j++)).get()));
        }
        cache.putMember(member.getLevel(), key, member);
        return member;
    }

    private void setOrderKey(
        Comparable orderKey, LevelColumnLayout layout, RolapMemberBase member)
    {
        if (layout.getOrderBySource() != NONE) {
            if (Util.deprecated(true, false)) {
                // Setting ordinals is wrong unless we're sure we're reading
                // the whole hierarchy.
                member.setOrdinal(lastOrdinal++);
            }
            member.setOrderKey(orderKey);
        }
    }

    static Comparable getCompositeKey(
        final Map<Object, SqlStatement.Accessor> accessors,
        final List<Integer> ordinals) throws SQLException
    {
        switch (ordinals.size()) {
        case 0:
            // Yes, there is a case where a level's ordinal is 0-ary. Its key
            // is the same as its parent level. Therefore there is only one
            // child per parent.
            return Util.COMPARABLE_EMPTY_LIST;
        case 1:
            Comparable o = accessors.get(ordinals.get(0)).get();
            return toComparable(o);
        default:
            return (Comparable) Util.flatList(
                new AbstractList<Comparable>() {
                    public Comparable get(int index) {
                        try {
                            final Comparable value =
                                accessors.get(ordinals.get(index)).get();
                            return toComparable(value);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    public int size() {
                        return ordinals.size();
                    }
                });
        }
    }

    public RolapMember allMember() {
        return hierarchy.getAllMember();
    }

    /**
     * <p>Looks up an object (and if needed, stores it) in a cached value pool.
     * This permits us to reuse references to an existing object rather than
     * create new references to what are essentially duplicates.  The intent
     * is to allow the duplicate object to be garbage collected earlier, thus
     * keeping overall memory requirements down.</p>
     *
     * <p>If
     * {@link mondrian.olap.MondrianProperties#SqlMemberSourceValuePoolFactoryClass}
     * is not set, then valuePool will be null and no attempt to cache the
     * value will be made.  The method will simply return the incoming
     * object reference.</p>
     *
     * @param incoming An object to look up.  Must be immutable in usage,
     *        even if not declared as such.
     * @return a reference to a cached object equal to the incoming object,
     *        or to the incoming object if either no cached object was found,
     *        or caching is disabled.
     */
    private <T> T getPooledValue(T incoming) {
        if (valuePool == null) {
            return incoming;
        } else {
            Object ret = this.valuePool.get(incoming);
            if (ret != null) {
                return (T) ret;
            } else {
                this.valuePool.put(incoming, incoming);
                return incoming;
            }
        }
    }

    /**
     * Generates the SQL to find all root members of a parent-child hierarchy.
     * For example,
     *
     * <blockquote>
     * <pre>SELECT "employee_id"
     * FROM "employee"
     * WHERE "supervisor_id" IS NULL
     * GROUP BY "employee_id"</pre>
     * </blockquote>
     *
     * <p>retrieves the root members of the <code>[Employee]</code>
     * hierarchy.</p>
     *
     * <p>Currently, parent-child hierarchies may have only one level (plus the
     * 'All' level).</p>
     */
    private String makeChildMemberSql_PCRoot(
        RolapMember member,
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder)
    {
        final SqlQueryBuilder queryBuilder =
            new SqlQueryBuilder(
                getDialect(),
                "while generating query to retrieve children of parent/child "
                + "hierarchy member " + member,
                layoutBuilder);

        assert member.isAll()
            : "In the current implementation, parent/child hierarchies must "
            + "have only one level (plus the 'All' level).";

        final RolapCubeLevel level = member.getLevel().getChildLevel();

        assert !level.isAll() : "all level cannot be parent-child";

        StringBuilder condition = new StringBuilder(64);
        for (RolapSchema.PhysColumn parentKey
            : level.getParentAttribute().getKeyList())
        {
            queryBuilder.addColumn(
                queryBuilder.column(parentKey, level.cubeDimension),
                Clause.FROM);
            String parentId = parentKey.toSql();
            condition.append(parentId);
        }
        final String nullParentValue = level.getNullParentValue();
        if (nullParentValue == null
            || nullParentValue.equalsIgnoreCase("NULL"))
        {
            condition.append(" IS NULL");
        } else {
            // Quote the value if it doesn't seem to be a number.
            try {
                Util.discard(Double.parseDouble(nullParentValue));
                condition.append(" = ");
                condition.append(nullParentValue);
            } catch (NumberFormatException e) {
                condition.append(" = ");
                Util.singleQuoteString(nullParentValue, condition);
            }
        }
        queryBuilder.sqlQuery.addWhere(condition.toString());
        return projectProperties(
            layoutBuilder, queryBuilder, level,
            level.attribute.getProperties(),
            Util.<RolapSchema.PhysColumn>identityFunctor());
    }

    /**
     * Generates the SQL statement to access the children of
     * <code>member</code> in a parent-child hierarchy. For example,
     * <blockquote>
     *
     * <pre>SELECT "employee_id"
     * FROM "employee"
     * WHERE "supervisor_id" = 5</pre>
     * </blockquote> retrieves the children of the member
     * <code>[Employee].[5]</code>.
     *
     * <p>See also {@link SqlTupleReader#makeLevelMembersSql}.
     */
    private String makeChildMemberSqlPC(
        RolapMember member,
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder)
    {
        final Dialect dialect = getDialect();
        final SqlQueryBuilder queryBuilder =
            new SqlQueryBuilder(
                dialect,
                "while generating query to retrieve children of "
                + "parent/child hierarchy member " + member,
                layoutBuilder);
        RolapCubeLevel level = member.getLevel();

        final RolapClosure closure = level.getClosure();
        boolean haveClosure = level.isParentChild() && closure != null;
        if (haveClosure) {
            level =
                Util.first(
                    (RolapCubeLevel) closure.closedPeerLevel,
                    level);
        }

        Util.assertTrue(!level.isAll(), "all level cannot be parent-child");

        RolapAttribute attribute =
            haveClosure ? level.getAttribute() : level.getParentAttribute();
        for (Pair<RolapSchema.PhysColumn, Comparable> pair
            : Pair.iterate(attribute.getKeyList(), member.getKeyAsList()))
        {
            RolapSchema.PhysColumn parentKey = pair.left;
            final Comparable keyVal = pair.right;
            SqlConstraintUtils.constrainLevel2(
                queryBuilder, parentKey, level.cubeDimension, keyVal);
        }

        // Add the distance column in the predicate, if it is available.
        if (closure != null
            && closure.distanceColumn != null)
        {
            queryBuilder.addColumn(
                queryBuilder.column(
                    closure.distanceColumn,
                    ((RolapCubeLevel) closure.closedPeerLevel).cubeDimension),
                Clause.FROM);
            queryBuilder.sqlQuery.addWhere(
                closure.distanceColumn.toSql() + " = 1");
        }

        RolapCubeLevel cubeLevel =
            haveClosure ? level.getChildLevel() : level;
        return projectProperties(
            layoutBuilder, queryBuilder, cubeLevel,
            member.getLevel().attribute.getProperties(),
            Util.<RolapSchema.PhysColumn>identityFunctor());
    }

    // implement MemberReader
    public RolapMember getLeadMember(RolapMember member, int n) {
        throw new UnsupportedOperationException();
    }

    public void getMemberRange(
        RolapLevel level,
        RolapMember startMember,
        RolapMember endMember,
        List<RolapMember> memberList)
    {
        throw new UnsupportedOperationException();
    }

    public int compare(
        RolapMember m1,
        RolapMember m2,
        boolean siblingsAreEqual)
    {
        throw new UnsupportedOperationException();
    }

    public TupleReader.MemberBuilder getMemberBuilder() {
        return this;
    }

    public RolapMember getDefaultMember() {
        // we expected the CacheMemberReader to implement this
        throw new UnsupportedOperationException();
    }

    public RolapMember getMemberParent(RolapMember member) {
        throw new UnsupportedOperationException();
    }

    // ~ -- Inner classes ------------------------------------------------------

    /**
     * Member of a parent-child dimension which has a closure table.
     *
     * <p>When looking up cells, this member will automatically be converted
     * to a corresponding member of the auxiliary dimension which maps onto
     * the closure table.
     */
    private static class RolapParentChildMember extends RolapMemberBase {
        private final RolapMember dataMember;
        private final int depth;

        public RolapParentChildMember(
            RolapMember parentMember,
            RolapCubeLevel childLevel,
            Comparable value,
            RolapMember dataMember)
        {
            super(
                parentMember, childLevel, value, dataMember.getMemberType(),
                deriveUniqueName(
                    parentMember, childLevel, dataMember.getName(), false),
                Larders.ofName(dataMember.getName()));
            this.dataMember = dataMember;
            this.depth = (parentMember != null)
                ? parentMember.getDepth() + 1
                : 0;
        }

        public RolapMember getDataMember() {
            return dataMember;
        }

        public int getDepth() {
            return depth;
        }

        public int getOrdinal() {
            return dataMember.getOrdinal();
        }
    }

    /**
     * Member of a parent-child dimension which has no closure table.
     *
     * <p>This member is calculated. When you ask for its value, it returns
     * an expression which aggregates the values of its child members.
     * This calculation is very inefficient, and we can only support
     * aggregatable measures ("count distinct" is non-aggregatable).
     * Unfortunately it's the best we can do without a closure table.
     */
    private static class RolapParentChildMemberNoClosure
        extends RolapParentChildMember
    {
        public RolapParentChildMemberNoClosure(
            RolapMember parentMember,
            RolapCubeLevel childLevel,
            Comparable value,
            RolapMember dataMember)
        {
            super(parentMember, childLevel, value, dataMember);
        }

        protected boolean computeCalculated(final MemberType memberType) {
            // NOTE: Although this member returns a calculation, we do want to
            // return it from <Level>.Members (which doesn't usually contain
            // calculated members).
            return false;
        }

        public boolean isEvaluated() {
            return true;
        }

        public Exp getExpression() {
            return getHierarchy().getAggregateChildrenExpression();
        }
    }

    /**
     * <p>Interface definition for the pluggable factory used to decide
     * which implementation of {@link java.util.Map} to use to pool
     * reusable values.</p>
     */
    public interface ValuePoolFactory {
        /**
         * <p>Create a new {@link java.util.Map} to be used to pool values.
         * The value pool permits us to reuse references to existing objects
         * rather than create new references to what are essentially duplicates
         * of the same object.  The intent is to allow the duplicate object
         * to be garbage collected earlier, thus keeping overall memory
         * requirements down.</p>
         *
         * @param source The {@link SqlMemberSource} in which values are
         * being pooled.
         * @return a new value pool map
         */
        Map<Object, Object> create(SqlMemberSource source);
    }

    /**
     * Default {@link mondrian.rolap.SqlMemberSource.ValuePoolFactory}
     * implementation, used if
     * {@link mondrian.olap.MondrianProperties#SqlMemberSourceValuePoolFactoryClass}
     * is not set.
     */
    public static final class NullValuePoolFactory
        implements ValuePoolFactory
    {
        /**
         * {@inheritDoc}
         * <p>This version returns null, meaning that
         * by default values will not be pooled.</p>
         *
         * @param source {@inheritDoc}
         * @return {@inheritDoc}
         */
        public Map<Object, Object> create(SqlMemberSource source) {
            return null;
        }
    }

    /**
     * <p>Creates the ValuePoolFactory which is in turn used
     * to create property-value maps for member properties.</p>
     *
     * <p>The name of the ValuePoolFactory is drawn from
     * {@link mondrian.olap.MondrianProperties#SqlMemberSourceValuePoolFactoryClass}
     * in mondrian.properties.  If unset, it defaults to
     * {@link mondrian.rolap.SqlMemberSource.NullValuePoolFactory}. </p>
     */
    public static final class ValuePoolFactoryFactory
        extends ObjectFactory.Singleton<ValuePoolFactory>
    {
        /**
         * Single instance of the <code>ValuePoolFactoryFactory</code>.
         */
        private static final ValuePoolFactoryFactory factory;
        static {
            factory = new ValuePoolFactoryFactory();
        }

        /**
         * Access the <code>ValuePoolFactory</code> instance.
         *
         * @return the <code>Map</code>.
         */
        public static ValuePoolFactory getValuePoolFactory() {
            return factory.getObject();
        }

        /**
         * The constructor for the <code>ValuePoolFactoryFactory</code>.
         * This passes the <code>ValuePoolFactory</code> class to the
         * <code>ObjectFactory</code> base class.
         */
        private ValuePoolFactoryFactory() {
            super(ValuePoolFactory.class);
        }

        protected StringProperty getStringProperty() {
            return MondrianProperties.instance()
               .SqlMemberSourceValuePoolFactoryClass;
        }

        protected ValuePoolFactory getDefault(
            Class[] parameterTypes,
            Object[] parameterValues)
            throws CreationException
        {
            return new NullValuePoolFactory();
        }
    }
}

// End SqlMemberSource.java
