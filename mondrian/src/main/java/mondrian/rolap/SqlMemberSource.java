/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2017 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.TupleList;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.*;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.server.monitor.SqlStatementEvent;
import mondrian.spi.Dialect;
import mondrian.util.*;

import org.apache.log4j.Logger;

import org.eigenbase.util.property.StringProperty;

import java.sql.*;
import java.util.*;

import javax.sql.DataSource;

/**
 * A <code>SqlMemberSource</code> reads members from a SQL database.
 *
 * <p>It's a good idea to put a {@link CacheMemberReader} on top of this.
 *
 * @author jhyde
 * @since 21 December, 2001
 */
class SqlMemberSource
    implements MemberReader, SqlTupleReader.MemberBuilder
{
    private static final Logger LOGGER =
        Logger.getLogger(SqlMemberSource.class);
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
        RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
        int count = 0;
        for (RolapLevel level : levels) {
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
        RolapLevel level,
        List<Comparable> keyValues)
    {
        if (level.isAll()) {
            return null;
        }
        List<Dialect.Datatype> datatypeList = new ArrayList<Dialect.Datatype>();
        List<MondrianDef.Expression> columnList =
            new ArrayList<MondrianDef.Expression>();
        for (RolapLevel x = level;; x = (RolapLevel) x.getParentLevel()) {
            columnList.add(x.getKeyExp());
            datatypeList.add(x.getDatatype());
            if (x.isUnique()) {
                break;
            }
        }
        final List<RolapMember> list =
            getMembersInLevel(
                level,
                new MemberKeyConstraint(
                    columnList,
                    datatypeList,
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

    public int getLevelMemberCount(RolapLevel level) {
        if (level.isAll()) {
            return 1;
        }
        return getMemberCount(level, dataSource);
    }

    private int getMemberCount(RolapLevel level, DataSource dataSource) {
        boolean[] mustCount = new boolean[1];
        String sql = makeLevelMemberCountSql(level, dataSource, mustCount);
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
     * <code>level</code>. For example, <blockquote>
     *
     * <pre>SELECT count(*) FROM (
     *   SELECT DISTINCT "country", "state_province"
     *   FROM "customer") AS "init"</pre>
     *
     * </blockquote> counts the non-leaf "state_province" level. MySQL
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
    private String makeLevelMemberCountSql(
        RolapLevel level,
        DataSource dataSource,
        boolean[] mustCount)
    {
        mustCount[0] = false;
        SqlQuery sqlQuery =
            SqlQuery.newQuery(
                dataSource,
                "while generating query to count members in level " + level);
        int levelDepth = level.getDepth();
        RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
        if (levelDepth == levels.length) {
            // "select count(*) from schema.customer"
            sqlQuery.addSelect("count(*)", null);
            hierarchy.addToFrom(sqlQuery, level.getKeyExp());
            return sqlQuery.toString();
        }
        if (!sqlQuery.getDialect().allowsFromQuery()) {
            List<String> columnList = new ArrayList<String>();
            int columnCount = 0;
            for (int i = levelDepth; i >= 0; i--) {
                RolapLevel level2 = levels[i];
                if (level2.isAll()) {
                     continue;
                }
                if (columnCount > 0) {
                    if (sqlQuery.getDialect().allowsCompoundCountDistinct()) {
                        // no op.
                    } else if (true) {
                        // for databases where both SELECT-in-FROM and
                        // COUNT DISTINCT do not work, we do not
                        // generate any count and do the count
                        // distinct "manually".
                        mustCount[0] = true;
                    }
                }
                hierarchy.addToFrom(sqlQuery, level2.getKeyExp());

                String keyExp = level2.getKeyExp().getExpression(sqlQuery);
                columnList.add(keyExp);

                if (level2.isUnique()) {
                    break; // no further qualification needed
                }
                ++columnCount;
            }
            if (mustCount[0]) {
                for (String colDef : columnList) {
                    final String exp =
                        sqlQuery.getDialect().generateCountExpression(colDef);
                    sqlQuery.addSelect(exp, null);
                    sqlQuery.addOrderBy(exp, true, false, true);
                }
            } else {
                int i = 0;
                StringBuilder sb = new StringBuilder();
                for (String colDef : columnList) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    i++;
                    sb.append(
                        sqlQuery.getDialect()
                            .generateCountExpression(colDef));
                }
                sqlQuery.addSelect(
                    "count(DISTINCT " + sb.toString() + ")", null);
            }
            return sqlQuery.toString();

        } else {
            sqlQuery.setDistinct(true);
            for (int i = levelDepth; i >= 0; i--) {
                RolapLevel level2 = levels[i];
                if (level2.isAll()) {
                    continue;
                }
                MondrianDef.Expression keyExp = level2.getKeyExp();
                hierarchy.addToFrom(sqlQuery, keyExp);
                sqlQuery.addSelect(keyExp.getExpression(sqlQuery), null);
                if (level2.isUnique()) {
                    break; // no further qualification needed
                }
            }
            SqlQuery outerQuery =
                SqlQuery.newQuery(
                    dataSource,
                    "while generating query to count members in level "
                    + level);
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
        Pair<String, List<SqlStatement.Type>> pair = makeKeysSql(dataSource);
        final String sql = pair.left;
        List<SqlStatement.Type> types = pair.right;
        RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
        SqlStatement stmt =
            RolapUtil.executeQuery(
                dataSource, sql, types, 0, 0,
                new SqlStatement.StatementLocus(
                    null,
                    "SqlMemberSource.getMembers",
                    "while building member cache",
                    SqlStatementEvent.Purpose.TUPLES, 0),
                -1, -1, null);
        try {
            final List<SqlStatement.Accessor> accessors = stmt.getAccessors();
            List<RolapMember> list = new ArrayList<RolapMember>();
            Map<MemberKey, RolapMember> map =
                new HashMap<MemberKey, RolapMember>();
            RolapMember root = null;
            if (hierarchy.hasAll()) {
                root = hierarchy.getAllMember();
                list.add(root);
            }

            int limit = MondrianProperties.instance().ResultLimit.get();
            ResultSet resultSet = stmt.getResultSet();
            Execution execution = Locus.peek().execution;
            while (resultSet.next()) {
                // Check if the MDX query was canceled.
                CancellationChecker.checkCancelOrTimeout(
                    ++stmt.rowCount, execution);

                if (limit > 0 && limit < stmt.rowCount) {
                    // result limit exceeded, throw an exception
                    throw stmt.handle(
                        MondrianResource.instance().MemberFetchLimitExceeded.ex(
                            limit));
                }

                int column = 0;
                RolapMember member = root;
                for (RolapLevel level : levels) {
                    if (level.isAll()) {
                        continue;
                    }
                    Object value = accessors.get(column).get();
                    if (value == null) {
                        value = RolapUtil.sqlNullValue;
                    }
                    RolapMember parent = member;
                    MemberKey key = new MemberKey(parent, value);
                    member = map.get(key);
                    if (member == null) {
                        RolapMemberBase memberBase =
                            new RolapMemberBase(parent, level, value);
                        memberBase.setOrdinal(lastOrdinal++);
                        member = memberBase;
/*
RME is this right
                        if (level.getOrdinalExp() != level.getKeyExp()) {
                            member.setOrdinal(lastOrdinal++);
                        }
*/
                        if (value == RolapUtil.sqlNullValue) {
                            addAsOldestSibling(list, member);
                        } else {
                            list.add(member);
                        }
                        map.put(key, member);
                    }
                    column++;

                    // REVIEW jvs 20-Feb-2007:  What about caption?

                    if (!level.getOrdinalExp().equals(level.getKeyExp())) {
                        if (assignOrderKeys) {
                            Object orderKey = accessors.get(column).get();
                            setOrderKey((RolapMemberBase) member, orderKey);
                        }
                        column++;
                    }

                    Property[] properties = level.getProperties();
                    for (Property property : properties) {
                        // REVIEW emcdermid 9-Jul-2009:
                        // Should we also look up the value in the
                        // pool here, rather than setting it directly?
                        // Presumably the value is already in the pool
                        // as a result of makeMember().
                        member.setProperty(
                            property.getName(),
                            accessors.get(column).get());
                        column++;
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
        member.setOrderKey((Comparable<?>) orderKey);
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

    private Pair<String, List<SqlStatement.Type>> makeKeysSql(
        DataSource dataSource)
    {
        SqlQuery sqlQuery =
            SqlQuery.newQuery(
                dataSource,
                "while generating query to retrieve members of " + hierarchy);
        RolapLevel[] levels = (RolapLevel[]) hierarchy.getLevels();
        for (RolapLevel level : levels) {
            if (level.isAll()) {
                continue;
            }
            final MondrianDef.Expression keyExp = level.getKeyExp();
            hierarchy.addToFrom(sqlQuery, keyExp);
            final String expString =
                keyExp.getExpression(sqlQuery);
            final String keyAlias =
                sqlQuery.addSelectGroupBy(expString, null);

            if (!keyExp.equals(level.getOrdinalExp())) {
                // Ordering comes from a separate expression
                final MondrianDef.Expression ordinalExp =
                    level.getOrdinalExp();
                // Make sure the table is selected.
                hierarchy.addToFrom(sqlQuery, ordinalExp);
                final String ordinalExpString =
                    ordinalExp.getExpression(sqlQuery);
                final String orderAlias =
                    sqlQuery.addSelectGroupBy(ordinalExpString, null);
                sqlQuery.addOrderBy(
                    ordinalExpString,
                    orderAlias,
                    true, false, true, true);
            } else {
                // Still need to order by key.
                sqlQuery.addOrderBy(
                    expString,
                    keyAlias,
                    true, false, true, true);
            }

            RolapProperty[] properties = level.getProperties();
            for (RolapProperty property : properties) {
                final MondrianDef.Expression propExpr = property.getExp();
                hierarchy.addToFrom(sqlQuery, propExpr);
                final String propStringExpr =
                    propExpr.getExpression(sqlQuery);
                final String propAlias =
                    sqlQuery.addSelect(propStringExpr, null);
                // Some dialects allow us to eliminate properties from the
                // group by that are functionally dependent on the level value
                if (!sqlQuery.getDialect().allowsSelectNotInGroupBy()
                    || !property.dependsOnLevelValue())
                {
                    sqlQuery.addGroupBy(propStringExpr, propAlias);
                }
            }
        }
        return sqlQuery.toSqlAndTypes();
    }

    // implement MemberReader
    public List<RolapMember> getMembersInLevel(
        RolapLevel level)
    {
        TupleConstraint constraint =
            sqlConstraintFactory.getLevelMembersConstraint(null);
        return getMembersInLevel(level, constraint);
    }

    public List<RolapMember> getMembersInLevel(
        RolapLevel level,
        TupleConstraint constraint)
    {
        if (level.isAll()) {
            return Collections.singletonList(hierarchy.getAllMember());
        }
        Dimension dimension = level.getDimension();
        boolean isHighCardinality = dimension.isHighCardinality();
        final TupleReader tupleReader = isHighCardinality
                ? new HighCardSqlTupleReader(constraint)
                : new SqlTupleReader(constraint);
        if (isHighCardinality) {
            LOGGER.warn(
                MondrianResource.instance()
                    .HighCardinalityInDimension.str(
                        dimension.getUniqueName()));
        }
        tupleReader.addLevelMembers(level, this, null);
        final TupleList tupleList =
            tupleReader.readMembers(dataSource, null, null);

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
        return getMembersInLevel((RolapLevel) hierarchy.getLevels()[0]);
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
     * <p>Note that this method is never called in the context of
     * virtual cubes, it is only called on regular cubes.
     *
     * <p>See also {@link SqlTupleReader#makeLevelMembersSql}.
     */
    Pair<String, List<SqlStatement.Type>> makeChildMemberSql(
        RolapMember member,
        DataSource dataSource,
        MemberChildrenConstraint constraint)
    {
        SqlQuery sqlQuery =
            SqlQuery.newQuery(
                dataSource,
                "while generating query to retrieve children of member "
                    + member);

        // If this is a non-empty constraint, it is more efficient to join to
        // an aggregate table than to the fact table. See whether a suitable
        // aggregate table exists.
        AggStar aggStar = chooseAggStar(constraint, member);

        // Create the condition, which is either the parent member or
        // the full context (non empty).
        constraint.addMemberConstraint(sqlQuery, null, aggStar, member);

        RolapLevel level = (RolapLevel) member.getLevel().getChildLevel();

        boolean levelCollapsed =
            (aggStar != null)
            && isLevelCollapsed(aggStar, (RolapCubeLevel)level);

        boolean multipleCols =
            SqlMemberSource.levelContainsMultipleColumns(level);

        if (levelCollapsed && !multipleCols) {
            // if this is a single column collapsed level, there is
            // no need to join it with dimension tables
            RolapStar.Column starColumn =
                ((RolapCubeLevel) level).getStarKeyColumn();
            int bitPos = starColumn.getBitPosition();
            AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
            String q = aggColumn.generateExprString(sqlQuery);
            final String qAlias =
                sqlQuery.addSelectGroupBy(q, starColumn.getInternalType());
            sqlQuery.addOrderBy(
                q, qAlias, true, false, true, true);
            aggColumn.getTable().addToFrom(sqlQuery, false, true);
            return sqlQuery.toSqlAndTypes();
        }

        hierarchy.addToFrom(sqlQuery, level.getKeyExp());
        String q = level.getKeyExp().getExpression(sqlQuery);
        String idAlias =
            sqlQuery.addSelectGroupBy(q, level.getInternalType());

        // in non empty mode the level table must be joined to the fact
        // table
        constraint.addLevelConstraint(sqlQuery, null, aggStar, level);

        if (levelCollapsed) {
            // if this is a collapsed level, add a join between key and aggstar
            RolapStar.Column starColumn =
                ((RolapCubeLevel) level).getStarKeyColumn();
            int bitPos = starColumn.getBitPosition();
            AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
            RolapStar.Condition condition =
                new RolapStar.Condition(
                    level.getKeyExp(),
                    aggColumn.getExpression());
            sqlQuery.addWhere(condition.toString(sqlQuery));
            hierarchy.addToFromInverse(sqlQuery, level.getKeyExp());

            // also may need to join parent levels to make selection unique
            RolapCubeLevel parentLevel = (RolapCubeLevel)level.getParentLevel();
            boolean isUnique = level.isUnique();
            while (parentLevel != null && !parentLevel.isAll() && !isUnique) {
                hierarchy.addToFromInverse(sqlQuery, parentLevel.getKeyExp());
                starColumn = parentLevel.getStarKeyColumn();
                bitPos = starColumn.getBitPosition();
                aggColumn = aggStar.lookupColumn(bitPos);
                condition =
                    new RolapStar.Condition(
                        parentLevel.getKeyExp(),
                        aggColumn.getExpression());
                sqlQuery.addWhere(condition.toString(sqlQuery));
                parentLevel = parentLevel.getParentLevel();
            }
        }

        if (level.hasCaptionColumn()) {
            MondrianDef.Expression captionExp = level.getCaptionExp();
            if (!levelCollapsed) {
                hierarchy.addToFrom(sqlQuery, captionExp);
            }
            String captionSql = captionExp.getExpression(sqlQuery);
            sqlQuery.addSelectGroupBy(captionSql, null);
        }
        if (!levelCollapsed) {
            hierarchy.addToFrom(sqlQuery, level.getOrdinalExp());
        }

        final String orderBy = level.getOrdinalExp().getExpression(sqlQuery);
        if (!orderBy.equals(q)) {
            String orderAlias = sqlQuery.addSelectGroupBy(orderBy, null);
            sqlQuery.addOrderBy(
                orderBy, orderAlias, true, false, true, true);
        } else {
            sqlQuery.addOrderBy(
                q, idAlias, true, false, true, true);
        }

        RolapProperty[] properties = level.getProperties();
        for (RolapProperty property : properties) {
            final MondrianDef.Expression exp = property.getExp();
            if (!levelCollapsed) {
                hierarchy.addToFrom(sqlQuery, exp);
            }
            final String s = exp.getExpression(sqlQuery);
            String alias = sqlQuery.addSelect(s, null);
            // Some dialects allow us to eliminate properties from the
            // group by that are functionally dependent on the level value
            if (!sqlQuery.getDialect().allowsSelectNotInGroupBy()
                || !property.dependsOnLevelValue())
            {
                sqlQuery.addGroupBy(s, alias);
            }
        }
        return sqlQuery.toSqlAndTypes();
    }

    private static AggStar chooseAggStar(
        MemberChildrenConstraint constraint,
        RolapMember member)
    {
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

        // Expand calculated so we don't miss their bitkeys
        final Member[] members =
            SqlConstraintUtils.expandSupportedCalculatedMembers(
                Arrays.asList(
                    evaluator.getNonAllMembers()),
                    evaluator)
                    .getMembersArray();

        // if measure is calculated, we can't continue
        if (!(members[0] instanceof RolapBaseCubeMeasure)) {
            return null;
        }
        RolapBaseCubeMeasure measure = (RolapBaseCubeMeasure)members[0];
        // we need to do more than this!  we need the rolap star ordinal, not
        // the rolap cube

        int bitPosition =
            ((RolapStar.Measure)measure.getStarMeasure()).getBitPosition();

        // childLevel will always end up being a RolapCubeLevel, but the API
        // calls into this method can be both shared RolapMembers and
        // RolapCubeMembers so this cast is necessary for now. Also note that
        // this method will never be called in the context of a virtual cube
        // so baseCube isn't necessary for retrieving the correct column

        // get the level using the current depth
        RolapCubeLevel childLevel =
            (RolapCubeLevel) member.getLevel().getChildLevel();

        RolapStar.Column column = childLevel.getStarKeyColumn();

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
        levelBitKey.set(column.getBitPosition());
        measureBitKey.set(bitPosition);

        // Set the bits for limited rollup members
        RolapUtil.constraintBitkeyForLimitedMembers(
            evaluator, members, cube, levelBitKey);

        // find the aggstar using the masks
        return AggregationManager.findAgg(
            star, levelBitKey, measureBitKey, new boolean[] {false});
    }

    /**
     * Determine if a level contains more than a single column for its
     * data, such as an ordinal column or property column
     *
     * @param level the level to check
     * @return true if multiple relational columns are involved in this level
     */
    public static boolean levelContainsMultipleColumns(RolapLevel level) {
        if (level.isAll()) {
            return false;
        }
        MondrianDef.Expression keyExp = level.getKeyExp();
        MondrianDef.Expression ordinalExp = level.getOrdinalExp();
        MondrianDef.Expression captionExp = level.getCaptionExp();

        if (!keyExp.equals(ordinalExp)) {
            return true;
        }

        if (captionExp != null && !keyExp.equals(captionExp)) {
            return true;
        }

        RolapProperty[] properties = level.getProperties();
        for (RolapProperty property : properties) {
            if (!property.getExp().equals(keyExp)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Determine if the given aggregate table has the dimension level
     * specified within in (AggStar.FactTable) it, aka collapsed,
     * or associated with foreign keys (AggStar.DimTable)
     *
     * @param aggStar aggregate star if exists
     * @param level level
     * @return true if agg table has level or not
     */
    public static boolean isLevelCollapsed(
        AggStar aggStar,
        RolapCubeLevel level)
    {
        boolean levelCollapsed = false;
        if (level.isAll()) {
            return levelCollapsed;
        }
        RolapStar.Column starColumn = level.getStarKeyColumn();
        int bitPos = starColumn.getBitPosition();
        AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
        if (aggColumn.getTable() instanceof AggStar.FactTable) {
            levelCollapsed = true;
        }
        return levelCollapsed;
    }

    public void getMemberChildren(
        List<RolapMember> parentMembers,
        List<RolapMember> children)
    {
        MemberChildrenConstraint constraint =
            sqlConstraintFactory.getMemberChildrenConstraint(null);
        getMemberChildren(parentMembers, children, constraint);
    }

    public Map<? extends Member, Access> getMemberChildren(
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
                getMembersInLevel(childLevel, lmc);
            children.addAll(list);
            return Util.toNullValuesMap(children);
        }

        // fetch them one by one
        for (RolapMember parentMember : parentMembers) {
            getMemberChildren(parentMember, children, mcc);
        }
        return Util.toNullValuesMap(children);
    }

    public void getMemberChildren(
        RolapMember parentMember,
        List<RolapMember> children)
    {
        MemberChildrenConstraint constraint =
            sqlConstraintFactory.getMemberChildrenConstraint(null);
        getMemberChildren(parentMember, children, constraint);
    }

    public Map<? extends Member, Access> getMemberChildren(
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
            return Util.toNullValuesMap((List)Collections.emptyList());
        }
        getMemberChildren2(parentMember, children, constraint);
        return Util.toNullValuesMap(children);
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
                childLevel = (RolapLevel) parentLevel.getChildLevel();
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
        Pair<String, List<SqlStatement.Type>> pair;
        boolean parentChild;
        final RolapLevel parentLevel = parentMember.getLevel();
        RolapLevel childLevel;
        if (parentLevel.isParentChild()) {
            pair = makeChildMemberSqlPC(parentMember);
            parentChild = true;
            childLevel = parentLevel;
        } else {
            childLevel = (RolapLevel) parentLevel.getChildLevel();
            if (childLevel == null) {
                // member is at last level, so can have no children
                return;
            }
            if (childLevel.isParentChild()) {
                pair = makeChildMemberSql_PCRoot(parentMember);
                parentChild = true;
            } else {
                pair = makeChildMemberSql(parentMember, dataSource, constraint);
                parentChild = false;
            }
        }
        final String sql = pair.left;

        int startChildrenSize = children.size();
        HashMap<RolapMember, Object> rolapToOrdinalMap = new HashMap<>();

        final List<SqlStatement.Type> types = pair.right;
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
            boolean checkCacheStatus = true;

            final List<SqlStatement.Accessor> accessors = stmt.getAccessors();
            ResultSet resultSet = stmt.getResultSet();
            RolapMember parentMember2 = RolapUtil.strip(parentMember);
            Execution execution = Locus.peek().execution;
            while (resultSet.next()) {
                // Check if the MDX query was canceled.
                CancellationChecker.checkCancelOrTimeout(
                    ++stmt.rowCount, execution);

                if (limit > 0 && limit < stmt.rowCount) {
                    // result limit exceeded, throw an exception
                    throw MondrianResource.instance().MemberFetchLimitExceeded
                        .ex(limit);
                }

                Object value = accessors.get(0).get();
                if (value == null) {
                    value = RolapUtil.sqlNullValue;
                }
                Object captionValue;
                int columnOffset = 1;
                if (childLevel.hasCaptionColumn()) {
                    // The columnOffset needs to take into account
                    // the caption column if one exists
                    captionValue = accessors.get(columnOffset++).get();
                } else {
                    captionValue = null;
                }
                Object key = cache.makeKey(parentMember2, value);
                RolapMember member = cache.getMember(key, checkCacheStatus);
                checkCacheStatus = false; /* Only check the first time */
                if (member == null) {
                    member =
                        makeMember(
                            parentMember2, childLevel, value, captionValue,
                            parentChild, stmt, key, columnOffset);
                }
                if (value == RolapUtil.sqlNullValue) {
                    children.toArray();
                    addAsOldestSibling(children, member);
                } else {
                    children.add(member);
                }

                if (!childLevel.getOrdinalExp()
                        .equals(childLevel.getKeyExp()))
                {
                    Object ordinal = accessors.get(columnOffset).get();
                    Object prevValue = rolapToOrdinalMap.put(member, ordinal);
                    if (prevValue != null && !Util.equals(prevValue, ordinal)) {
                        LOGGER.error(
                            "Column expression for "
                            + member.getUniqueName()
                            + " is inconsistent with ordinal or caption expression."
                            + " It should have 1:1 relationship");
                    }
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
        Object value,
        Object captionValue,
        boolean parentChild,
        SqlStatement stmt,
        Object key,
        int columnOffset)
        throws SQLException
    {
        final RolapLevel rolapChildLevel;
        if (childLevel instanceof RolapCubeLevel) {
            rolapChildLevel = ((RolapCubeLevel) childLevel).getRolapLevel();
        } else {
            rolapChildLevel = childLevel;
        }
        RolapMemberBase member =
            new RolapMemberBase(parentMember, rolapChildLevel, value);
        if (!childLevel.getOrdinalExp().equals(childLevel.getKeyExp())) {
            member.setOrdinal(lastOrdinal++);
        }
        if (captionValue != null) {
            // passing caption column raw value
            // to be properly formatted later
            member.setCaptionValue(captionValue);
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
                    parentMember, rolapChildLevel, value, member)
                : new RolapParentChildMemberNoClosure(
                    parentMember, rolapChildLevel, value, member);
        }
        Property[] properties = childLevel.getProperties();
        final List<SqlStatement.Accessor> accessors = stmt.getAccessors();
        if (!childLevel.getOrdinalExp().equals(childLevel.getKeyExp())) {
            if (assignOrderKeys) {
                Object orderKey = accessors.get(columnOffset).get();
                setOrderKey(member, orderKey);
            }
            ++columnOffset;
        }
        for (int j = 0; j < properties.length; j++) {
            Property property = properties[j];
            member.setProperty(
                property.getName(),
                getPooledValue(accessors.get(columnOffset + j).get()));
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
    private Pair<String, List<SqlStatement.Type>> makeChildMemberSql_PCRoot(
        RolapMember member)
    {
        SqlQuery sqlQuery =
            SqlQuery.newQuery(
                dataSource,
                "while generating query to retrieve children of parent/child "
                + "hierarchy member " + member);
        Util.assertTrue(
            member.isAll(),
            "In the current implementation, parent/child hierarchies must "
            + "have only one level (plus the 'All' level).");

        RolapLevel level = (RolapLevel) member.getLevel().getChildLevel();

        Util.assertTrue(!level.isAll(), "all level cannot be parent-child");
        Util.assertTrue(
            level.isUnique(), "parent-child level '"
                + level + "' must be unique");

        hierarchy.addToFrom(sqlQuery, level.getParentExp());
        String parentId = level.getParentExp().getExpression(sqlQuery);
        StringBuilder condition = new StringBuilder(64);
        condition.append(parentId);
        if (level.getNullParentValue() == null
            || level.getNullParentValue().equalsIgnoreCase("NULL"))
        {
            condition.append(" IS NULL");
        } else {
            // Quote the value if it doesn't seem to be a number.
            try {
                Util.discard(Double.parseDouble(level.getNullParentValue()));
                condition.append(" = ");
                condition.append(level.getNullParentValue());
            } catch (NumberFormatException e) {
                condition.append(" = ");
                Util.singleQuoteString(level.getNullParentValue(), condition);
            }
        }
        sqlQuery.addWhere(condition.toString());

        addLevel(sqlQuery, level, true);

        return sqlQuery.toSqlAndTypes();
    }

    private void addLevel(
        SqlQuery sqlQuery,
        RolapLevel level,
        boolean group)
    {
        final MondrianDef.Expression key = level.getKeyExp();
        final MondrianDef.Expression order = level.getOrdinalExp();

        // Make sure the tables are in the query.
        hierarchy.addToFrom(sqlQuery, key);
        hierarchy.addToFrom(sqlQuery, order);

        // First deal with the key column.
        final String keySql = key.getExpression(sqlQuery);
        final String keyAlias =
            group
                ? sqlQuery.addSelectGroupBy(keySql, level.getInternalType())
                : sqlQuery.addSelect(keySql, level.getInternalType());

        // Now deal with the ordering column.
        final String orderSql = order.getExpression(sqlQuery);
        if (!orderSql.equals(keySql)) {
            final String orderAlias =
                group
                    ? sqlQuery.addSelectGroupBy(orderSql, null)
                    : sqlQuery.addSelect(orderSql, null);
            sqlQuery.addOrderBy(
                orderSql,
                orderAlias,
                true, false, true, true);
        } else {
            // Same key as order. Just order it.
            sqlQuery.addOrderBy(
                keySql,
                keyAlias,
                true, false, true, true);
        }

        final RolapProperty[] properties = level.getProperties();
        for (RolapProperty property : properties) {
            final MondrianDef.Expression exp = property.getExp();
            hierarchy.addToFrom(sqlQuery, exp);
            final String s = exp.getExpression(sqlQuery);
            // REVIEW: Maybe use property.getType?
            String alias = sqlQuery.addSelect(s, null);
            // Some dialects allow us to eliminate properties from the group by
            // that are functionally dependent on the level value
            if (group
                && (!sqlQuery.getDialect().allowsSelectNotInGroupBy()
                    || !property.dependsOnLevelValue()))
            {
                sqlQuery.addGroupBy(s, alias);
            }
        }
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
    private Pair<String, List<SqlStatement.Type>> makeChildMemberSqlPC(
        RolapMember member)
    {
        SqlQuery sqlQuery =
            SqlQuery.newQuery(
                dataSource,
                "while generating query to retrieve children of "
                + "parent/child hierarchy member " + member);
        RolapLevel level = member.getLevel();

        Util.assertTrue(!level.isAll(), "all level cannot be parent-child");
        Util.assertTrue(
            level.isUnique(),
            "parent-child level '" + level + "' must be "  + "unique");

        hierarchy.addToFrom(sqlQuery, level.getParentExp());
        String parentId = level.getParentExp().getExpression(sqlQuery);

        StringBuilder buf = new StringBuilder();
        sqlQuery.getDialect().quote(buf, member.getKey(), level.getDatatype());
        sqlQuery.addWhere(parentId, " = ", buf.toString());

        hierarchy.addToFrom(sqlQuery, level.getKeyExp());
        String childId = level.getKeyExp().getExpression(sqlQuery);
        String idAlias =
            sqlQuery.addSelectGroupBy(childId, level.getInternalType());
        hierarchy.addToFrom(sqlQuery, level.getOrdinalExp());
        final String orderBy = level.getOrdinalExp().getExpression(sqlQuery);
        if (!orderBy.equals(childId)) {
            String orderAlias = sqlQuery.addSelectGroupBy(orderBy, null);
            sqlQuery.addOrderBy(
                orderBy, orderAlias, true, false, true, true);
        } else {
            sqlQuery.addOrderBy(
                childId, idAlias, true, false, true, true);
        }

        RolapProperty[] properties = level.getProperties();
        for (RolapProperty property : properties) {
            final MondrianDef.Expression exp = property.getExp();
            hierarchy.addToFrom(sqlQuery, exp);
            final String s = exp.getExpression(sqlQuery);
            String alias = sqlQuery.addSelect(s, null);
            // Some dialects allow us to eliminate properties from the group by
            // that are functionally dependent on the level value
            if (!sqlQuery.getDialect().allowsSelectNotInGroupBy()
                || !property.dependsOnLevelValue())
            {
                sqlQuery.addGroupBy(s, alias);
            }
        }
        return sqlQuery.toSqlAndTypes();
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

        @Override
        public boolean isParentChildPhysicalMember() {
            return true;
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
