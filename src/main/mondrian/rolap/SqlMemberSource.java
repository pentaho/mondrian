/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 December, 2001
*/
package mondrian.rolap;

import mondrian.calc.TupleList;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.sql.*;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;
import mondrian.server.Locus;
import mondrian.spi.Dialect;
import mondrian.util.CreationException;
import mondrian.util.ObjectFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

import mondrian.util.Pair;
import org.eigenbase.util.property.StringProperty;

/**
 * A <code>SqlMemberSource</code> reads members from a SQL database.
 *
 * <p>It's a good idea to put a {@link CacheMemberReader} on top of this.
 *
 * @author jhyde
 * @since 21 December, 2001
 * @version $Id$
 */
class SqlMemberSource
    implements MemberReader, SqlTupleReader.MemberBuilder
{
    private final SqlConstraintFactory sqlConstraintFactory =
        SqlConstraintFactory.instance();
    private final RolapHierarchy hierarchy;
    private final DataSource dataSource;
    private MemberCache cache;
    private int lastOrdinal = 0;
    private boolean assignOrderKeys;
    private Map<Object, Object> valuePool;

    SqlMemberSource(RolapHierarchy hierarchy) {
        this.hierarchy = hierarchy;
        this.dataSource =
            hierarchy.getRolapSchema().getInternalConnection().getDataSource();
        assignOrderKeys =
            MondrianProperties.instance().CompareSiblingsByOrderKey.get();
        valuePool = ValuePoolFactoryFactory.getValuePoolFactory().create(this);
    }

    // implement MemberSource
    public RolapHierarchy getHierarchy() {
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
        for (RolapLevel level : hierarchy.getRolapLevelList()) {
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

    public RolapMember lookupMember(
        List<Id.Segment> uniqueNameParts,
        boolean failIfNotFound)
    {
        throw new UnsupportedOperationException();
    }

    public int getLevelMemberCount(RolapLevel level) {
        if (level.isAll()) {
            return 1;
        }
        return getMemberCount(level, dataSource);
    }

    private int getMemberCount(RolapLevel level, DataSource dataSource) {
        boolean[] mustCount = new boolean[1];
        String sql =
            makeAttributeMemberCountSql(
                level.attribute, dataSource, mustCount);
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
                        if (!colStr.equals(colStrings[i])) {
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
        DataSource dataSource,
        boolean[] mustCount)
    {
        mustCount[0] = false;
        SqlQuery sqlQuery =
            SqlQuery.newQuery(
                dataSource,
                "while generating query to count members in attribute "
                + attribute);
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder =
            new SqlTupleReader.ColumnLayoutBuilder(null);
        final RolapSchema.SqlQueryBuilder queryBuilder =
            new RolapSchema.SqlQueryBuilder(sqlQuery, layoutBuilder);
        if (!sqlQuery.getDialect().allowsFromQuery()) {
            StringBuilder columnList = new StringBuilder();
            int columnCount = 0;
            for (RolapSchema.PhysColumn column : attribute.keyList) {
                if (columnCount > 0) {
                    if (sqlQuery.getDialect().allowsCompoundCountDistinct()) {
                        columnList.append(", ");
                    } else {
                        // for databases where both SELECT-in-FROM and
                        // COUNT DISTINCT do not work, we do not
                        // generate any count and do the count
                        // distinct "manually".
                        mustCount[0] = true;
                    }
                }
                queryBuilder.addToFrom(column);

                String keyExp = column.toSql();
                if (columnCount > 0
                    && !sqlQuery.getDialect().allowsCompoundCountDistinct()
                    && sqlQuery.getDialect().getDatabaseProduct()
                    == Dialect.DatabaseProduct.SYBASE)
                {
                    keyExp = "convert(varchar, " + columnList + ")";
                }
                columnList.append(keyExp);
                ++columnCount;
            }
            if (mustCount[0]) {
                final String str = columnList.toString();
                sqlQuery.addSelect(str, null);
                sqlQuery.addOrderBy(str, true, false, true);
            } else {
                sqlQuery.addSelect(
                    "count(DISTINCT " + columnList + ")", null);
            }
            return sqlQuery.toString();

        } else {
            sqlQuery.setDistinct(true);
            for (RolapSchema.PhysColumn column : attribute.keyList) {
                queryBuilder.addToFrom(column);
                sqlQuery.addSelect(column.toSql(), column.getInternalType());
            }
            SqlQuery outerQuery =
                SqlQuery.newQuery(
                    dataSource,
                    "while generating query to count members in attribute "
                    + attribute);
            outerQuery.addSelect("count(*)", null);
            // Note: the "init" is for Postgres, which requires
            // FROM-queries to have an alias
            boolean failIfExists = true;
            outerQuery.addFrom(sqlQuery, "init", failIfExists);
            return outerQuery.toString();
        }
    }


    public List<RolapMember> getMembers() {
        return getMembers(dataSource);
    }

    private List<RolapMember> getMembers(DataSource dataSource) {
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder =
            new SqlTupleReader.ColumnLayoutBuilder(
                hierarchy.levelList.get(hierarchy.levelList.size() - 1)
                    .attribute.keyList);
        String sql = makeKeysSql(dataSource, layoutBuilder);
        List<SqlStatement.Type> types = layoutBuilder.types;
        SqlStatement stmt =
            RolapUtil.executeQuery(
                dataSource, sql, types, 0, 0,
                new Locus(
                    null,
                    "SqlMemberSource.getMembers",
                    "while building member cache"),
                -1, -1);
        final SqlTupleReader.ColumnLayout columnLayout =
            layoutBuilder.toLayout();
        try {
            final List<SqlStatement.Accessor> accessors = stmt.getAccessors();
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
                for (RolapLevel level : hierarchy.getRolapLevelList()) {
                    if (level.isAll()) {
                        continue;
                    }
                    final SqlTupleReader.LevelColumnLayout levelLayout =
                        columnLayout.levelLayoutList.get(level.getDepth());
                    // TODO: pre-allocate these, one per level; remember to
                    // clone list (using Flat2List or Flat3List if appropriate)
                    final Object[] keyValues =
                        new Object[level.attribute.keyList.size()];

                    // It's cheaper to reuse the same list for probing the
                    // hashmap. Composite keys are stored using a different
                    // kind of list, but the lists should be comparable.
                    final List<Object> keyList = Arrays.asList(keyValues);

                    for (int i = 0; i < levelLayout.keyOrdinals.length; i++) {
                        int keyOrdinal = levelLayout.keyOrdinals[i];
                        Object value = accessors.get(keyOrdinal).get();
                        if (value == null) {
                            value = RolapUtil.sqlNullValue;
                        }
                        keyValues[i] = value;
                    }
                    RolapMember parent = member;

                    final Object key =
                        keyValues.length == 1 ? keyValues[0] : keyList;
                    member = map.get(key);
                    if (member == null) {
                        final Object keyClone =
                            RolapMember.Key.create(keyValues);
                        RolapMemberBase memberBase =
                            new RolapMemberBase(parent, level, keyClone);
                        memberBase.setOrdinal(lastOrdinal++);
                        member = memberBase;
                        list.add(member);
                        map.put(keyClone, member);
                    }

                    // REVIEW jvs 20-Feb-2007:  What about caption? TODO:

                    if (levelLayout.ordinalOrdinal >= 0) {
                        if (assignOrderKeys) {
                            Object orderKey =
                                accessors.get(levelLayout.ordinalOrdinal).get();
                            setOrderKey((RolapMemberBase) member, orderKey);
                        }
                    }

                    int i = 0;
                    for (Property property : level.attribute.getProperties()) {
                        int propertyOrdinal = levelLayout.propertyOrdinals[i++];
                        // REVIEW emcdermid 9-Jul-2009:
                        // Should we also look up the value in the
                        // pool here, rather than setting it directly?
                        // Presumably the value is already in the pool
                        // as a result of makeMember().
                        member.setProperty(
                            property.getName(),
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

    private void setOrderKey(RolapMemberBase member, Object orderKey) {
        if ((orderKey != null) && !(orderKey instanceof Comparable)) {
            orderKey = orderKey.toString();
        }
        member.setOrderKey((Comparable) orderKey);
    }

    /**
     * Adds <code>member</code> just before the first element in
     * <code>list</code> which has the same parent.
     */
    private void addAsOldestSibling(
        List<RolapMember> list,
        RolapMember member)
    {
        int i = list.size();
        while (--i >= 0) {
            RolapMember sibling = list.get(i);
            if (sibling.getParentMember() != member.getParentMember()) {
                break;
            }
        }
        list.add(i + 1, member);
    }

    enum Sgo {
        SELECT_ORDER, SELECT, SELECT_GROUP, SELECT_GROUP_ORDER;

        public Sgo maybeGroup(boolean needsGroupBy) {
            if (needsGroupBy) {
                switch (this) {
                case SELECT_ORDER:
                    return SELECT_GROUP_ORDER;
                case SELECT:
                    return SELECT_GROUP;
                }
            }
            return this;
        }
    }

    private String makeKeysSql(
        DataSource dataSource,
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder)
    {
        SqlQuery sqlQuery =
            SqlQuery.newQuery(
                dataSource,
                "while generating query to retrieve members of " + hierarchy);
        final RolapSchema.SqlQueryBuilder queryBuilder =
            new RolapSchema.SqlQueryBuilder(sqlQuery, layoutBuilder);
        for (RolapLevel level : hierarchy.getRolapLevelList()) {
            for (RolapSchema.PhysColumn column : level.attribute.keyList) {
                queryBuilder.asasdasd(column, Sgo.SELECT_GROUP);
            }
            for (RolapSchema.PhysColumn column : level.attribute.orderByList) {
                queryBuilder.asasdasd(column, Sgo.SELECT_ORDER);
            }
            for (RolapProperty property : level.attribute.getProperties()) {
                if (property.attribute == null) {
                    continue;
                }
                for (RolapSchema.PhysColumn column : property.attribute.keyList)
                {
                    final Sgo sgo;
                    if (sqlQuery.getDialect().allowsSelectNotInGroupBy()) {
                         sgo = Sgo.SELECT;
                    } else {
                        // Some dialects allow us to eliminate properties from
                        // the group by that are functionally dependent on the
                        // level value.
                        sgo = Sgo.SELECT_GROUP;
                    }
                    queryBuilder.asasdasd(column, sgo);
                }
            }
        }
        return sqlQuery.toSql();
    }

    // implement MemberReader
    public List<RolapMember> getMembersInLevel(
        RolapLevel level,
        int startOrdinal,
        int endOrdinal)
    {
        TupleConstraint constraint =
            sqlConstraintFactory.getLevelMembersConstraint(null);
        return getMembersInLevel(level, startOrdinal, endOrdinal, constraint);
    }

    public List<RolapMember> getMembersInLevel(
        RolapLevel level,
        int startOrdinal,
        int endOrdinal,
        TupleConstraint constraint)
    {
        if (level.isAll()) {
            final List<RolapMember> list = new ArrayList<RolapMember>();
            list.add(hierarchy.getAllMember());
            Util.deprecated("why is line below commented out?", false);
            //return Collections.singletonList(hierarchy.getAllMember());
            return list;
        }
        return getMembersInLevel(level, constraint);
    }

    private List<RolapMember> getMembersInLevel(
        RolapLevel level,
        TupleConstraint constraint)
    {
        final TupleReader tupleReader =
            level.getDimension().isHighCardinality()
                ? new HighCardSqlTupleReader(constraint)
                : new SqlTupleReader(constraint);
        tupleReader.addLevelMembers(level, this, null);
        final TupleList tupleList =
            tupleReader.readTuples(dataSource, null, null);

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
        return getMembersInLevel(
            hierarchy.getRolapLevelList().get(0), 0, Integer.MAX_VALUE);
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
     */
    String makeChildMemberSql(
        RolapMember member,
        DataSource dataSource,
        final MemberChildrenConstraint constraint,
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder)
    {
        Util.deprecated(
            "make caption, key, name etc. properties of a level so can handle in a loop",
            false);
        SqlQuery sqlQuery =
            SqlQuery.newQuery(
                dataSource,
                "while generating query to retrieve children of member "
                + member);
        final RolapSchema.SqlQueryBuilder queryBuilder =
            new RolapSchema.SqlQueryBuilder(sqlQuery, layoutBuilder);

        // If this is a non-empty constraint, it is more efficient to join to
        // an aggregate table than to the fact table. See whether a suitable
        // aggregate table exists.
        AggStar aggStar = chooseAggStar(constraint, member);

        // Create the condition, which is either the parent member or
        // the full context (non empty).
        final RolapStarSet starSet = constraint.createStarSet();
        constraint.addMemberConstraint(sqlQuery, starSet, aggStar, member);

        RolapLevel level = member.getLevel().getChildLevel();
        boolean levelCollapsed =
            (aggStar != null)
            && isLevelCollapsed(
                aggStar, (RolapCubeLevel) level, starSet.getMeasureGroup());
        layoutBuilder.createLayoutFor(level);

        // If constraint is 'anchored' to a fact table, add join conditions to
        // the fact table (via the table containing the dimension's key, if
        // the dimension is a snowflake). Otherwise just add the path from the
        // dimension's key.
        for (RolapSchema.PhysColumn column : level.attribute.keyList) {
            final RolapSchema.PhysPath keyPath =
                level.getDimension().getKeyPath(column);
            keyPath.addToFrom(sqlQuery, false);
        }
        if (starSet.getMeasureGroup() != null) {
            final RolapSchema.PhysPath path =
                starSet.getMeasureGroup().getPath(level.getDimension());
            for (RolapSchema.PhysHop hop : path.hopList) {
                sqlQuery.addFrom(hop.relation, null, false);
                sqlQuery.addWhere(hop.link.sql);
            }
        }
        for (RolapSchema.PhysColumn column : level.attribute.keyList) {
            // REVIEW: also need to join each attr to dim key?
            final String sql = column.toSql();
            int ordinal = layoutBuilder.lookup(sql);
            if (ordinal < 0) {
                queryBuilder.addToFrom(column);
                final String alias =
                    sqlQuery.addSelectGroupBy(sql, column.getInternalType());
                ordinal = layoutBuilder.register(sql, alias);
            }
            layoutBuilder.currentLevelLayout.keyOrdinalList.add(ordinal);
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
        constraint.addLevelConstraint(
            sqlQuery, starSet, aggStar, level);

        if (levelCollapsed) {
            // if this is a collapsed level, add a join between key and aggstar;
            // also may need to join parent levels to make selection unique
            final RolapCubeLevel cubeLevel = (RolapCubeLevel) level;
            final RolapMeasureGroup measureGroup = starSet.getMeasureGroup();
            for (RolapSchema.PhysColumn column : level.attribute.keyList) {
                hierarchy.addToFromInverse(sqlQuery, column);
                RolapStar.Column starColumn =
                    measureGroup.getRolapStarColumn(
                        cubeLevel.cubeDimension,
                        column,
                        false);
                int bitPos = starColumn.getBitPosition();
                AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
                RolapStar.Condition condition =
                    new RolapStar.Condition(
                        column,
                        aggColumn.getExpression());
                sqlQuery.addWhere(condition.toString(sqlQuery));
            }
        }

        if (level.attribute.nameExp != null) {
            RolapSchema.PhysColumn nameExp = level.attribute.nameExp;
            if (false) {
                queryBuilder.addToFrom(nameExp);
            }
            String nameSql = nameExp.toSql();
            int ordinal = layoutBuilder.lookup(nameSql);
            if (ordinal < 0) {
                final String alias =
                    sqlQuery.addSelectGroupBy(
                        nameSql, nameExp.getInternalType());
                ordinal = layoutBuilder.register(nameSql, alias);
            }
            layoutBuilder.currentLevelLayout.nameOrdinal = ordinal;
        }

        if (level.attribute.captionExp != null) {
            RolapSchema.PhysColumn captionExp = level.attribute.captionExp;
            if (false) {
                queryBuilder.addToFrom(captionExp);
            }
            String captionSql = captionExp.toSql();
            int ordinal = layoutBuilder.lookup(captionSql);
            if (ordinal < 0) {
                final String alias =
                    sqlQuery.addSelectGroupBy(
                        captionSql, captionExp.getInternalType());
                ordinal = layoutBuilder.register(captionSql, alias);
            }
            layoutBuilder.currentLevelLayout.captionOrdinal = ordinal;
        }

        for (RolapSchema.PhysColumn key : level.attribute.orderByList) {
            // TODO: join in ordinal relation if different
            queryBuilder.addToFrom(key);
            String orderBy = key.toSql();
            int ordinal = layoutBuilder.lookup(orderBy);
            if (ordinal < 0) {
                final String alias = sqlQuery.addSelectGroupBy(orderBy, null);
                ordinal = layoutBuilder.register(orderBy, alias);
            }
            sqlQuery.addOrderBy(orderBy, true, false, true);
            layoutBuilder.currentLevelLayout.ordinalList.add(ordinal);
        }

        for (RolapProperty property : level.attribute.getProperties()) {
            // TODO: properties that are composite, or have key != name exp
            final RolapSchema.PhysExpr exp = property.attribute.nameExp;
            queryBuilder.addToFrom(exp);
            final String s = exp.toSql();
            int ordinal = layoutBuilder.lookup(s);
            // Some dialects allow us to eliminate properties from the
            // group by that are functionally dependent on the level value
            if (ordinal < 0) {
                final String alias;
                if (!sqlQuery.getDialect().allowsSelectNotInGroupBy()
                    || !property.dependsOnLevelValue())
                {
                    alias = sqlQuery.addSelectGroupBy(s, null);
                } else {
                    alias = sqlQuery.addSelect(s, null);
                }
                ordinal = layoutBuilder.register(s, alias);
            }
            layoutBuilder.currentLevelLayout.propertyOrdinalList.add(ordinal);
        }
        Pair<String, List<SqlStatement.Type>> pair = sqlQuery.toSqlAndTypes();
        layoutBuilder.types.addAll(pair.right);
        return pair.left;
    }

    // TODO: move method somewhere better
//    private RolapSchema.PhysPath getPath(
//        RolapSchema.PhysSchemaGraph graph,
//        RolapSchema.PhysColumn exp,
//        RolapSchema.PhysColumn exp1)
//    {
//        RolapSchema.PhysSchema physSchema = null;
//        RolapSchema.PhysPathBuilder builder =
//            new RolapSchema.PhysPathBuilder(exp.relation);
//        return graph.findPath(
//            exp.relation,
//            exp1.relation);
//    }

    private static AggStar chooseAggStar(
        MemberChildrenConstraint constraint,
        RolapMember member)
    {
        if (Util.deprecated(true, false)) {
            return null; // FIXME
        }
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

        int bitPosition =
            ((RolapStar.Measure) measure.getStarMeasure()).getBitPosition();
        int ordinal = measure.getOrdinal();

        // childLevel will always end up being a RolapCubeLevel, but the API
        // calls into this method can be both shared RolapMembers and
        // RolapCubeMembers so this cast is necessary for now. Also note that
        // this method will never be called in the context of a virtual cube
        // so baseCube isn't necessary for retrieving the correct column

        // get the level using the current depth
        RolapCubeLevel childLevel =
            (RolapCubeLevel) member.getLevel().getChildLevel();

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
        return AggregationManager.instance().findAgg(
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
        RolapLevel childLevel =
            getCommonChildLevelForDescendants(parentMembers);
        if (childLevel != null) {
            TupleConstraint lmc =
                sqlConstraintFactory.getDescendantsConstraint(
                    parentMembers, mcc);
            List<RolapMember> list =
                getMembersInLevel(childLevel, 0, Integer.MAX_VALUE, lmc);
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
    private RolapLevel getCommonChildLevelForDescendants(
        List<RolapMember> parents)
    {
        // at least two members required
        if (parents.size() < 2) {
            return null;
        }
        RolapLevel parentLevel = null;
        RolapLevel childLevel = null;
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
        RolapMember parentMember,
        List<RolapMember> children,
        MemberChildrenConstraint constraint)
    {
        final String sql;
        boolean parentChild;
        final RolapLevel parentLevel = parentMember.getLevel();
        RolapLevel childLevel;
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder =
            new SqlTupleReader.ColumnLayoutBuilder(
                parentLevel.attribute.keyList);
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
                    parentMember, dataSource, constraint, layoutBuilder);
            }
        }
        final List<SqlStatement.Type> types = layoutBuilder.types;
        SqlStatement stmt =
            RolapUtil.executeQuery(
                dataSource, sql, types, 0, 0,
                new Locus(
                    Locus.peek().execution,
                    "SqlMemberSource.getMemberChildren",
                    "while building member cache"),
                -1, -1);
        try {
            int limit = MondrianProperties.instance().ResultLimit.get();
            boolean checkCacheStatus = true;

            final List<SqlStatement.Accessor> accessors = stmt.getAccessors();
            ResultSet resultSet = stmt.getResultSet();
            RolapMember parentMember2 = RolapUtil.strip(parentMember);
            final SqlTupleReader.ColumnLayout fullLayout =
                layoutBuilder.toLayout();
            final SqlTupleReader.LevelColumnLayout layout =
                fullLayout.levelLayoutList.get(childLevel.getDepth());
            while (resultSet.next()) {
                ++stmt.rowCount;
                if (limit > 0 && limit < stmt.rowCount) {
                    // result limit exceeded, throw an exception
                    throw MondrianResource.instance().MemberFetchLimitExceeded
                        .ex(limit);
                }

                Object[] keys = new Object[layout.keyOrdinals.length];
                List<Object> keyList = Arrays.asList(keys);
                for (int i = 0; i < layout.keyOrdinals.length; i++) {
                    Object value = accessors.get(layout.keyOrdinals[i]).get();
                    if (value == null) {
                        value = RolapUtil.sqlNullValue;
                    }
                    keys[i] = value;
                }
                Object captionValue;
                if (layout.captionOrdinal >= 0) {
                    captionValue = accessors.get(layout.captionOrdinal).get();
                } else {
                    captionValue = null;
                }
                Object key = keys.length == 1 ? keys[0] : keyList;
                RolapMember member = cache.getMember(key, checkCacheStatus);
                checkCacheStatus = false; // only check the first time
                if (member == null) {
                    Object keyClone = RolapMember.Key.create(keys);
                    member =
                        makeMember(
                            parentMember2, childLevel, keyClone, captionValue,
                            parentChild, stmt, layout);
                }
                if (Util.deprecated(false, false)
                    /* value == RolapUtil.sqlNullValue */)
                {
                    children.toArray();
                    addAsOldestSibling(children, member);
                } else {
                    children.add(member);
                }
            }
        } catch (SQLException e) {
            throw stmt.handle(e);
        } finally {
            stmt.close();
        }
    }

    public RolapMember makeMember(
        RolapMember parentMember,
        RolapLevel childLevel,
        Object key,
        Object captionValue,
        boolean parentChild,
        SqlStatement stmt,
        SqlTupleReader.LevelColumnLayout layout)
        throws SQLException
    {
        final RolapLevel rolapChildLevel;
        if (childLevel instanceof RolapCubeLevel) {
            rolapChildLevel = ((RolapCubeLevel) childLevel).getRolapLevel();
        } else {
            rolapChildLevel = childLevel;
        }
        final String name = String.valueOf(
            getPooledValue(
                stmt.getAccessors().get(layout.nameOrdinal).get()));
        RolapMemberBase member =
            new RolapMemberBase(
                parentMember, rolapChildLevel, key,
                name, Member.MemberType.REGULAR);
        if (layout.hasOrdinal) {
            member.setOrdinal(lastOrdinal++);
        }
        if (captionValue != null) {
            member.setCaption(captionValue.toString());
        }
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
        }
        final List<RolapProperty>
            properties = childLevel.attribute.getProperties();
        final List<SqlStatement.Accessor> accessors = stmt.getAccessors();
        if (layout.hasOrdinal) {
            if (assignOrderKeys) {
                Object orderKey =
                    accessors.get(layout.ordinalOrdinal).get();
                setOrderKey(member, orderKey);
            }
        }
        int j = 0;
        for (RolapProperty property : properties) {
            member.setProperty(
                property.getName(),
                getPooledValue(
                    accessors.get(layout.propertyOrdinals[j++]).get()));
        }
        cache.putMember(key, member);
        return member;
    }

    public RolapMember allMember() {
        final RolapHierarchy rolapHierarchy =
            hierarchy instanceof RolapCubeHierarchy
                ? ((RolapCubeHierarchy) hierarchy).getRolapHierarchy()
                : hierarchy;
        return rolapHierarchy.getAllMember();
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
    private Object getPooledValue(Object incoming) {
        if (valuePool == null) {
            return incoming;
        } else {
            Object ret = this.valuePool.get(incoming);
            if (ret != null) {
                return ret;
            } else {
                this.valuePool.put(incoming, incoming);
                return incoming;
            }
        }
    }

    /**
     * Generates the SQL to find all root members of a parent-child hierarchy.
     * For example, <blockquote>
     *
     * <pre>SELECT "employee_id"
     * FROM "employee"
     * WHERE "supervisor_id" IS NULL
     * GROUP BY "employee_id"</pre>
     * </blockquote> retrieves the root members of the <code>[Employee]</code>
     * hierarchy.
     *
     * <p>Currently, parent-child hierarchies may have only one level (plus the
     * 'All' level).
     */
    private String makeChildMemberSql_PCRoot(
        RolapMember member,
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder)
    {
        SqlQuery sqlQuery =
            SqlQuery.newQuery(
                dataSource,
                "while generating query to retrieve children of parent/child "
                + "hierarchy member " + member);
        final RolapSchema.SqlQueryBuilder queryBuilder =
            new RolapSchema.SqlQueryBuilder(sqlQuery, layoutBuilder);
        Util.assertTrue(
            member.isAll(),
            "In the current implementation, parent/child hierarchies must "
            + "have only one level (plus the 'All' level).");

        RolapLevel level = member.getLevel().getChildLevel();

        Util.assertTrue(!level.isAll(), "all level cannot be parent-child");

        StringBuilder condition = new StringBuilder(64);
        for (RolapSchema.PhysColumn parentKey
            : level.attribute.parentAttribute.keyList)
        {
            queryBuilder.addToFrom(parentKey);
            String parentId = parentKey.toSql();
            condition.append(parentId);
        }
        final String nullParentValue =
            level.attribute.parentAttribute.nullValue;
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
        sqlQuery.addWhere(condition.toString());
        for (RolapSchema.PhysColumn key : level.attribute.keyList) {
            queryBuilder.addToFrom(key);
            String childId = key.toSql();
            final String alias = sqlQuery.addSelectGroupBy(childId, null);
        }
        for (RolapSchema.PhysColumn key : level.attribute.orderByList) {
            queryBuilder.addToFrom(key);
            String orderBy = key.toSql();
            int ordinal = layoutBuilder.lookup(orderBy);
            if (ordinal < 0) {
                final String alias = sqlQuery.addSelectGroupBy(orderBy, null);
                ordinal = layoutBuilder.register(orderBy, alias);
            }
            layoutBuilder.currentLevelLayout.ordinalList.add(ordinal);
            sqlQuery.addOrderBy(orderBy, true, false, true);
        }

        for (RolapProperty property : level.attribute.getProperties()) {
            // TODO: properties have key, ordinal, etc., not just name. To do
            // this property, we should store just the property's key in the
            // member, then use the property key to look up the property as a
            // member.  But for now assume that properties have non-composite
            // keys and their name is the same.
            assert property.attribute.keyList.size() == 1 : "FIXME";
            final RolapSchema.PhysExpr exp = property.attribute.nameExp;
            queryBuilder.addToFrom(exp);
            final String s = exp.toSql();
            String alias = sqlQuery.addSelect(s, null);
            // Some dialects allow us to eliminate properties from the group by
            // that are functionally dependent on the level value
            if (!sqlQuery.getDialect().allowsSelectNotInGroupBy()
                || !property.dependsOnLevelValue())
            {
                sqlQuery.addGroupBy(s, alias);
            }
        }
        return sqlQuery.toSql();
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
        SqlQuery sqlQuery =
            SqlQuery.newQuery(
                dataSource,
                "while generating query to retrieve children of "
                + "parent/child hierarchy member " + member);
        final RolapSchema.SqlQueryBuilder queryBuilder =
            new RolapSchema.SqlQueryBuilder(sqlQuery, layoutBuilder);
        RolapLevel level = member.getLevel();

        Util.assertTrue(!level.isAll(), "all level cannot be parent-child");

        for (int i = 0, keyListSize =
            level.attribute.parentAttribute.keyList.size(); i < keyListSize;
            i++)
        {
            RolapSchema.PhysColumn parentKey =
                level.attribute.parentAttribute.keyList.get(i);
            final RolapSchema.PhysColumn key = level.attribute.keyList.get(i);
            queryBuilder.addToFrom(parentKey);
            String parentId = parentKey.toSql();
            StringBuilder buf = new StringBuilder();
            sqlQuery.getDialect().quote(
                buf, member.getKeyAsList().get(i), key.getDatatype());
            sqlQuery.addWhere(parentId, " = ", buf.toString());
        }

        for (RolapSchema.PhysColumn key : level.attribute.keyList) {
            queryBuilder.addToFrom(key);
            String childId = key.toSql();
            final String alias =
                sqlQuery.addSelectGroupBy(childId, key.getInternalType());
            layoutBuilder.register(childId, alias);
        }
        for (RolapSchema.PhysColumn key : level.attribute.orderByList) {
            queryBuilder.addToFrom(key);
            String orderBy = key.toSql();
            sqlQuery.addOrderBy(orderBy, true, false, true);
            final int ordinal = layoutBuilder.lookup(orderBy);
            if (ordinal < 0) {
                final String alias =
                    sqlQuery.addSelectGroupBy(orderBy, key.getInternalType());
                layoutBuilder.register(orderBy, alias);
            }
        }

        for (RolapProperty property : level.attribute.getProperties()) {
            // TODO: create relationship to member representing prop value
            final RolapSchema.PhysExpr exp = property.attribute.nameExp;
            queryBuilder.addToFrom(exp);
            final String s = exp.toSql();
            String alias = sqlQuery.addSelect(s, null);
            // Some dialects allow us to eliminate properties from the group by
            // that are functionally dependent on the level value
            if (!sqlQuery.getDialect().allowsSelectNotInGroupBy()
                || !property.dependsOnLevelValue())
            {
                sqlQuery.addGroupBy(s, alias);
            }
        }
        return sqlQuery.toSql();
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
        private int depth = 0;

        public RolapParentChildMember(
            RolapMember parentMember,
            RolapLevel childLevel,
            Object value,
            RolapMember dataMember)
        {
            super(parentMember, childLevel, value);
            this.dataMember = dataMember;
            this.depth = (parentMember != null)
                ? parentMember.getDepth() + 1
                : 0;
        }

        public Member getDataMember() {
            return dataMember;
        }

        /**
         * @return the members's depth
         * @see mondrian.olap.Member#getDepth()
         */
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
            RolapLevel childLevel, Object value, RolapMember dataMember)
        {
            super(parentMember, childLevel, value, dataMember);
        }

        protected boolean computeCalculated(final MemberType memberType) {
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
