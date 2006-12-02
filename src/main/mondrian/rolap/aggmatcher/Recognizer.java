/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 30 August, 2001
*/

package mondrian.rolap.aggmatcher;

import mondrian.olap.Hierarchy;
import mondrian.olap.Dimension;
import mondrian.olap.MondrianDef;
import mondrian.olap.Cube;
import mondrian.resource.MondrianResource;
import mondrian.recorder.MessageRecorder;
import mondrian.rolap.RolapStar;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapSchema;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapAggregator;
import mondrian.rolap.HierarchyUsage;
import mondrian.rolap.sql.SqlQuery;

import java.util.*;

import org.apache.log4j.Logger;

/**
 * Abstract Recognizer class used to determine if a candidate aggregate table
 * has the column categories: "fact_count" column, measure columns, foreign key
 * and level columns.
 *
 * <p>Derived classes use either the default or explicit column descriptions in
 * matching column categories. The basic matching algorithm is in this class
 * while some specific column category matching and column building must be
 * specified in derived classes.
 *
 * <p>A Recognizer is created per candidate aggregate table. The tables columns are
 * then categorized. All errors and warnings are added to a MessageRecorder.
 *
 * <p>This class is less about defining a type and more about code sharing.
 *
 * @author Richard M. Emberson
 * @version $Id$
 */
abstract class Recognizer {

    private static final MondrianResource mres = MondrianResource.instance();
    private static final Logger LOGGER = Logger.getLogger(Recognizer.class);
    /**
     * This is used to wrap column name matching rules.
     */
    public interface Matcher {

        /**
         * Return true it the name matches and false otherwise.
         */
        boolean matches(String name);
    }

    protected final RolapStar star;
    protected final JdbcSchema.Table dbFactTable;
    protected final JdbcSchema.Table aggTable;
    protected final MessageRecorder msgRecorder;
    protected boolean returnValue;

    protected Recognizer(final RolapStar star,
                         final JdbcSchema.Table dbFactTable,
                         final JdbcSchema.Table aggTable,
                         final MessageRecorder msgRecorder) {
        this.star = star;
        this.dbFactTable = dbFactTable;
        this.aggTable = aggTable;
        this.msgRecorder = msgRecorder;

        returnValue = true;
    }

    /**
     * Return true if the candidate aggregate table was successfully mapped into
     * the fact table. This is the top-level checking method.
     * <p>
     * It first checks the ignore columns.
     * <p>
     * Next, the existence of a fact count column is checked.
     * <p>
     * Then the measures are checked. First the specified (defined,
     * explicit) measures are all determined. There must be at least one such
     * measure. This if followed by checking for implied measures (e.g., if base
     * fact table as both sum and average of a column and the aggregate has a
     * sum measure, the there is an implied average measure in the aggregate).
     * <p>
     * Now the levels are checked. This is in two parts. First, foreign keys are
     * checked followed by level columns (for collapsed dimension aggregates).
     * <p>
     * If eveything checks out, returns true.
     */
    public boolean check() {
        checkIgnores();
        checkFactCount();

        // Check measures
        int nosMeasures = checkMeasures();
        // There must be at least one measure
        checkNosMeasures(nosMeasures);
        generateImpliedMeasures();

        // Check levels
        List<JdbcSchema.Table.Column.Usage> notSeenForeignKeys = checkForeignKeys();
//printNotSeenForeignKeys(notSeenForeignKeys);
        checkLevels(notSeenForeignKeys);

        if (returnValue) {
            // Add all unused columns as warning to the MessageRecorder
            checkUnusedColumns();
        }

        return returnValue;
    }

    /**
     * Return the ignore column Matcher.
     */
    protected abstract Matcher getIgnoreMatcher();

    /**
     * Check all columns to be marked as ignore.
     */
    protected void checkIgnores() {
        Matcher ignoreMatcher = getIgnoreMatcher();

        for (JdbcSchema.Table.Column aggColumn : aggTable.getColumns()) {
            if (ignoreMatcher.matches(aggColumn.getName())) {
                makeIgnore(aggColumn);
            }
        }
    }

    /**
     * Create an ignore usage for the aggColumn.
     *
     * @param aggColumn
     */
    protected void makeIgnore(final JdbcSchema.Table.Column aggColumn) {
        JdbcSchema.Table.Column.Usage usage =
                aggColumn.newUsage(JdbcSchema.IGNORE_COLUMN_USAGE);
        usage.setSymbolicName("Ignore");
    }



    /**
     * Return the fact count column Matcher.
     */
    protected abstract Matcher getFactCountMatcher();

    /**
     * Make sure that the aggregate table has one fact count column and that its
     * type is numeric.
     */
    protected void checkFactCount() {
        msgRecorder.pushContextName("Recognizer.checkFactCount");

        try {

            Matcher factCountMatcher = getFactCountMatcher();

            int nosOfFactCounts = 0;
            for (JdbcSchema.Table.Column aggColumn : aggTable.getColumns()) {
                // if marked as ignore, then do not consider
                if (aggColumn.hasUsage(JdbcSchema.IGNORE_COLUMN_USAGE)) {
                    continue;
                }
                if (factCountMatcher.matches(aggColumn.getName())) {
                    if (aggColumn.getDatatype().isNumeric()) {
                        makeFactCount(aggColumn);
                        nosOfFactCounts++;
                    } else {
                        String msg = mres.NonNumericFactCountColumn.str(
                                aggTable.getName(),
                                dbFactTable.getName(),
                                aggColumn.getName(),
                                aggColumn.getTypeName());
                        msgRecorder.reportError(msg);

                        returnValue = false;
                    }
                }

            }
            if (nosOfFactCounts == 0) {
                String msg = mres.NoFactCountColumns.str(
                        aggTable.getName(),
                        dbFactTable.getName());
                msgRecorder.reportError(msg);

                returnValue = false;

            } else if (nosOfFactCounts > 1) {
                String msg = mres.TooManyFactCountColumns.str(
                        aggTable.getName(),
                        dbFactTable.getName(),
                        nosOfFactCounts);
                msgRecorder.reportError(msg);

                returnValue = false;
            }

        } finally {
            msgRecorder.popContextName();
        }
    }


    /**
     * Check all measure columns returning the number of measure columns.
     */
    protected abstract int checkMeasures();

    /**
     * Create a fact count usage for the aggColumn.
     *
     * @param aggColumn
     */
    protected void makeFactCount(final JdbcSchema.Table.Column aggColumn) {
        JdbcSchema.Table.Column.Usage usage =
                aggColumn.newUsage(JdbcSchema.FACT_COUNT_COLUMN_USAGE);
        usage.setSymbolicName("Fact Count");
    }


    /**
     * Make sure there was at least one measure column identified.
     *
     * @param nosMeasures
     */
    protected void checkNosMeasures(int nosMeasures) {
        msgRecorder.pushContextName("Recognizer.checkNosMeasures");

        try {
            if (nosMeasures == 0) {
                String msg = mres.NoMeasureColumns.str(
                        aggTable.getName(),
                        dbFactTable.getName()
                    );
                msgRecorder.reportError(msg);

                returnValue = false;
            }

        } finally {
            msgRecorder.popContextName();
        }
    }

    /**
     * An implied measure in an aggregate table is one where there is both a sum
     * and average measures in the base fact table and the aggregate table has
     * either a sum or average, the other measure is implied and can be
     * generated from the measure and the fact_count column.
     * <p>
     * For each column in the fact table, get its measure usages. If there is
     * both an average and sum aggregator associated with the column, then
     * iterator over all of the column usage of type measure of the aggregator
     * table. If only one aggregate column usage measure is found and this
     * RolapStar.Measure measure instance variable is the same as the
     * the fact table's usage's instance variable, then the other measure is
     * implied and the measure is created for the aggregate table.
     */
    protected void generateImpliedMeasures() {
        for (JdbcSchema.Table.Column factColumn : aggTable.getColumns()) {
            JdbcSchema.Table.Column.Usage sumFactUsage = null;
            JdbcSchema.Table.Column.Usage avgFactUsage = null;

            for (Iterator<JdbcSchema.Table.Column.Usage> mit =
                    factColumn.getUsages(JdbcSchema.MEASURE_COLUMN_USAGE);
                    mit.hasNext(); ) {
                JdbcSchema.Table.Column.Usage factUsage = mit.next();
                if (factUsage.getAggregator() == RolapAggregator.Avg) {
                    avgFactUsage = factUsage;
                } else if (factUsage.getAggregator() == RolapAggregator.Sum) {
                    sumFactUsage = factUsage;
                }
            }

            if (avgFactUsage != null && sumFactUsage != null) {
                JdbcSchema.Table.Column.Usage sumAggUsage = null;
                JdbcSchema.Table.Column.Usage avgAggUsage = null;
                int seenCount = 0;
                for (Iterator<JdbcSchema.Table.Column.Usage> mit =
                    aggTable.getColumnUsages(JdbcSchema.MEASURE_COLUMN_USAGE);
                        mit.hasNext(); ) {

                    JdbcSchema.Table.Column.Usage aggUsage = mit.next();
                    if (aggUsage.rMeasure == avgFactUsage.rMeasure) {
                        avgAggUsage = aggUsage;
                        seenCount++;
                    } else if (aggUsage.rMeasure == sumFactUsage.rMeasure) {
                        sumAggUsage = aggUsage;
                        seenCount++;
                    }
                }
                if (seenCount == 1) {
                    if (avgAggUsage != null) {
                        makeMeasure(sumFactUsage, avgAggUsage);
                    }
                    if (sumAggUsage != null) {
                        makeMeasure(avgFactUsage, sumAggUsage);
                    }
                }
            }
        }
    }

    /**
     * Here we have the fact usage of either sum or avg and an aggregate usage
     * of the opposite type. We wish to make a new aggregate usage based
     * on the existing usage's column of the same type as the fact usage.
     *
     * @param factUsage fact usage
     * @param aggSiblingUsage existing sibling usage
     */
    protected void makeMeasure(final JdbcSchema.Table.Column.Usage factUsage,
                               final JdbcSchema.Table.Column.Usage aggSiblingUsage) {
        JdbcSchema.Table.Column aggColumn = aggSiblingUsage.getColumn();

        JdbcSchema.Table.Column.Usage aggUsage =
            aggColumn.newUsage(JdbcSchema.MEASURE_COLUMN_USAGE);

        aggUsage.setSymbolicName(factUsage.getSymbolicName());
        RolapAggregator ra = convertAggregator(
                        aggUsage,
                        factUsage.getAggregator(),
                        aggSiblingUsage.getAggregator());
        aggUsage.setAggregator(ra);
        aggUsage.rMeasure = factUsage.rMeasure;
    }

    /**
     * This method creates an aggregate table column measure usage from a fact
     * table column measure usage.
     *
     * @param factUsage
     * @param aggColumn
     */
    protected void makeMeasure(final JdbcSchema.Table.Column.Usage factUsage,
                               final JdbcSchema.Table.Column aggColumn) {
        JdbcSchema.Table.Column.Usage aggUsage =
            aggColumn.newUsage(JdbcSchema.MEASURE_COLUMN_USAGE);

        aggUsage.setSymbolicName(factUsage.getSymbolicName());
        RolapAggregator ra =
                convertAggregator(aggUsage, factUsage.getAggregator());
        aggUsage.setAggregator(ra);
        aggUsage.rMeasure = factUsage.rMeasure;
    }

    /**
     * This method determine how may aggregate table column's match the fact
     * table foreign key column return in the number matched. For each matching
     * column a foreign key usage is created.
     */
    protected abstract int matchForeignKey(JdbcSchema.Table.Column.Usage factUsage);

    /**
     * This method checks the foreign key columns.
     * <p>
     * For each foreign key column usage in the fact table, determine how many
     * aggregate table columns match that column usage. If there is more than
     * one match, then that is an error. If there were no matches, then the
     * foreign key usage is added to the list of fact column foreign key that
     * were not in the aggregate table. This list is returned by this method.
     * <p>
     * This matches foreign keys that were not "lost" or "collapsed".
     *
     * @return  list on not seen foreign key column usages
     */
    protected List<JdbcSchema.Table.Column.Usage> checkForeignKeys() {
        msgRecorder.pushContextName("Recognizer.checkForeignKeys");

        try {

            List<JdbcSchema.Table.Column.Usage> notSeenForeignKeys =
                Collections.emptyList();

            for (Iterator<JdbcSchema.Table.Column.Usage> it =
                dbFactTable.getColumnUsages(JdbcSchema.FOREIGN_KEY_COLUMN_USAGE);
                    it.hasNext(); ) {

                JdbcSchema.Table.Column.Usage factUsage = it.next();

                int matchCount = matchForeignKey(factUsage);

                if (matchCount > 1) {
                    String msg = mres.TooManyMatchingForeignKeyColumns.str(
                            aggTable.getName(),
                            dbFactTable.getName(),
                            matchCount,
                            factUsage.getColumn().getName()
                        );
                    msgRecorder.reportError(msg);

                    returnValue = false;

                } else if (matchCount == 0) {
                    if (notSeenForeignKeys.isEmpty()) {
                        notSeenForeignKeys = new ArrayList<JdbcSchema.Table.Column.Usage>();
                    }
                    notSeenForeignKeys.add(factUsage);
                }
            }
            return notSeenForeignKeys;

        } finally {
            msgRecorder.popContextName();
        }
    }

    /**
     * This method identifies those columns in the aggregate table that match
     * "collapsed" dimension columns. Remember that a collapsed dimension is one
     * where the higher levels of some hierarchy are columns in the aggregate
     * table (and all of the lower levels are missing - it has aggregated up to
     * the first existing level).
     * <p>
     * Here, we do not start from the fact table, we iterator over each cube.
     * For each of the cube's dimensions, the dimension's hirarchies are
     * iterated over. In turn, each hierarchy's usage is iterated over.
     * if the hierarchy's usage's foreign key is not in the list of not seen
     * foreign keys (the notSeenForeignKeys parameter), then that hierarchy is
     * not considered. If the hierarchy's usage's foreign key is in the not seen
     * list, then starting with the hierarchy's top level, it is determined if
     * the combination of hierarchy, hierarchy usage, and level matches an
     * aggregated table column. If so, then a level usage is created for that
     * column and the hierarchy's next level is considered and so on until a
     * for a level an aggregate table column does not match. Then we continue
     * iterating over the hierarchy usages.
     * <p>
     * This check is different. The others mine the fact table usages. This
     * looks through the fact table's cubes' dimension, hierarchy,
     * hiearchy usages, levels to match up symbolic names for levels. The other
     * checks match on "physical" characteristics, the column name; this matches
     * on "logical" characteristics.
     * <p>
     * Note: Levels should not be created for foreign keys that WERE seen.
     * Currently, this is NOT checked explicitly. For the explicit rules any
     * extra columns MUST ge declared ignored or one gets an error.
     *
     * @param notSeenForeignKeys
     */
    protected void checkLevels(List<JdbcSchema.Table.Column.Usage> notSeenForeignKeys) {

        // These are the factTable that do not appear in the aggTable.
        // 1) find all cubes with this given factTable
        // 1) per cube, find all usages with the column as foreign key
        // 2) for each usage, find dimension and its levels
        // 3) determine if level columns are represented

        // In generaly, there is only one cube.
        for (RolapCube cube : findCubes()) {
            Dimension[] dims = cube.getDimensions();
            // start dimensions at 1 (0 is measures)
            for (int j = 1; j < dims.length; j++) {
                Dimension dim = dims[j];
                // Ok, got dimension.
                // See if any of the levels exist as columns in the
                // aggTable. This requires applying a map from:
                //   hierarchyName
                //   levelName
                //   levelColumnName
                // to each "unassigned" column in the aggTable.
                // Remember that the rule is if a level does appear,
                // then all of the higher levels must also appear.
                String dimName = dim.getName();

                Hierarchy[] hierarchies = dim.getHierarchies();
                for (Hierarchy hierarchy : hierarchies) {
                    HierarchyUsage[] hierarchyUsages =
                        cube.getUsages(hierarchy);
                    for (HierarchyUsage hierarchyUsage : hierarchyUsages) {
                        // Search through the notSeenForeignKeys list
                        // making sure that this HierarchyUsage's
                        // foreign key is not in the list.
                        String foreignKey = hierarchyUsage.getForeignKey();
                        boolean b = inNotSeenForeignKeys(
                            foreignKey,
                            notSeenForeignKeys);
                        if (!b) {
                            // It was not in the not seen list, so ignore
                            continue;
                        }


                        RolapLevel[] levels =
                            (RolapLevel[]) hierarchy.getLevels();
                        // If the top level is seen, then one or more
                        // lower levels may appear but there can be no
                        // missing levels between the top level and
                        // lowest level seen.
                        // On the other hand, if the top level is not
                        // seen, then no other levels should be present.
                        mid_level:
                        for (RolapLevel level : levels) {
                            if (level.isAll()) {
                                continue mid_level;
                            }
                            if (matchLevel(hierarchy, hierarchyUsage, level)) {

                                continue mid_level;

                            } else {
                                // There were no matches, break
                                // For now, do not check lower levels
                                break mid_level;
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Return true if the foreignKey column name is in the list of not seen
     * foreign keys.
     */
    boolean inNotSeenForeignKeys(
        String foreignKey,
        List<JdbcSchema.Table.Column.Usage> notSeenForeignKeys)
    {
        for (JdbcSchema.Table.Column.Usage usage : notSeenForeignKeys) {
            if (usage.getColumn().getName().equals(foreignKey)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Debug method: Print out not seen foreign key list.
     *
     * @param notSeenForeignKeys
     */
    private void printNotSeenForeignKeys(List notSeenForeignKeys) {
        LOGGER.debug("Recognizer.printNotSeenForeignKeys: "
            + aggTable.getName());
        for (Iterator it = notSeenForeignKeys.iterator(); it.hasNext(); ) {
            JdbcSchema.Table.Column.Usage usage =
                (JdbcSchema.Table.Column.Usage) it.next();
            LOGGER.debug("  " + usage.getColumn().getName());
        }
    }

    /**
     * Here a measure ussage is created and the right join condition is
     * explicitly supplied. This is needed is when the aggregate table's column
     * names may not match those found in the RolapStar.
     *
     * @param factUsage
     * @param aggColumn
     * @param rightJoinConditionColumnName
     */
    protected void makeForeignKey(final JdbcSchema.Table.Column.Usage factUsage,
                                  final JdbcSchema.Table.Column aggColumn,
                                  final String rightJoinConditionColumnName) {
        JdbcSchema.Table.Column.Usage aggUsage =
            aggColumn.newUsage(JdbcSchema.FOREIGN_KEY_COLUMN_USAGE);
        aggUsage.setSymbolicName("FOREIGN_KEY");
        // Extract from RolapStar enough stuff to build
        // AggStar subtable except the column name of the right join
        // condition might be different
        aggUsage.rTable = factUsage.rTable;
        aggUsage.rightJoinConditionColumnName = rightJoinConditionColumnName;

        aggUsage.rColumn = factUsage.rColumn;
    }

    /**
     * Match a aggregate table column given the hierarchy, hierarchy usage, and
     * rolap level returning true if a match is found.
     */
    protected abstract boolean matchLevel(
            final Hierarchy hierarchy,
            final HierarchyUsage hierarchyUsage,
            final RolapLevel level);

    /**
     * Make a level column usage.
     *
     * <p> Note there is a check in this code. If a given aggregate table
     * column has already has a level usage, then that usage must all refer to
     * the same hierarchy usage join table and column name as the one that
     * calling this method was to create. If there is an existing level usage
     * for the column and it matches something else, then it is an error.
     */
    protected void makeLevel(
            final JdbcSchema.Table.Column aggColumn,
            final Hierarchy hierarchy,
            final HierarchyUsage hierarchyUsage,
            final String factColumnName,
            final String levelColumnName,
            final String symbolicName) {

        msgRecorder.pushContextName("Recognizer.makeLevel");

        try {

        if (aggColumn.hasUsage(JdbcSchema.LEVEL_COLUMN_USAGE)) {
            // The column has at least one usage of level type
            // make sure we are looking at the
            // same table and column
            for (Iterator uit =
                aggColumn.getUsages(JdbcSchema.LEVEL_COLUMN_USAGE);
                    uit.hasNext(); ) {
                JdbcSchema.Table.Column.Usage aggUsage =
                    (JdbcSchema.Table.Column.Usage) uit.next();

                MondrianDef.Relation rel = hierarchyUsage.getJoinTable();
                String cName = levelColumnName;

                if (! aggUsage.relation.equals(rel) ||
                    ! aggColumn.column.name.equals(cName)) {

                    // this is an error so return
                    String msg = mres.DoubleMatchForLevel.str(
                        aggTable.getName(),
                        dbFactTable.getName(),
                        aggColumn.getName(),
                        aggUsage.relation.toString(),
                        aggColumn.column.name,
                        rel.toString(),
                        cName);
                    msgRecorder.reportError(msg);

                    returnValue = false;

                    msgRecorder.throwRTException();
                }
            }
        } else {
            JdbcSchema.Table.Column.Usage aggUsage =
                    aggColumn.newUsage(JdbcSchema.LEVEL_COLUMN_USAGE);
            // Cache table and column for the above
            // check
            aggUsage.relation = hierarchyUsage.getJoinTable();
            aggUsage.joinExp = hierarchyUsage.getJoinExp();
            aggUsage.levelColumnName = levelColumnName;

            aggUsage.setSymbolicName(symbolicName);

            String tableAlias = null;
            if (aggUsage.joinExp instanceof MondrianDef.Column) {
                MondrianDef.Column mcolumn =
                        (MondrianDef.Column) aggUsage.joinExp;
                tableAlias = mcolumn.table;
            } else {
                tableAlias = aggUsage.relation.getAlias();
            }


            RolapStar.Table factTable = star.getFactTable();
            RolapStar.Table descTable = factTable.findDescendant(tableAlias);

            if (descTable == null) {
                // TODO: what to do here???
                StringBuilder buf = new StringBuilder(256);
                buf.append("descendant table is null for factTable=");
                buf.append(factTable.getAlias());
                buf.append(", tableAlias=");
                buf.append(tableAlias);
                msgRecorder.reportError(buf.toString());

                returnValue = false;

                msgRecorder.throwRTException();
            }

            RolapStar.Column rc = descTable.lookupColumn(factColumnName);

            if (rc == null) {
                rc = lookupInChildren(descTable, factColumnName);

            }
            if (rc == null) {
                StringBuilder buf = new StringBuilder(256);
                buf.append("Rolap.Column not found (null) for tableAlias=");
                buf.append(tableAlias);
                buf.append(", factColumnName=");
                buf.append(factColumnName);
                buf.append(", levelColumnName=");
                buf.append(levelColumnName);
                buf.append(", symbolicName=");
                buf.append(symbolicName);
                msgRecorder.reportError(buf.toString());

                returnValue = false;

                msgRecorder.throwRTException();
            } else {
                aggUsage.rColumn = rc;
            }
        }
        } finally {
            msgRecorder.popContextName();
        }
    }

    protected RolapStar.Column lookupInChildren(
        final RolapStar.Table table,
        final String factColumnName)
    {
        // This can happen if we are looking at a collapsed dimension
        // table, and the collapsed dimension in question in the
        // fact table is a snowflake (not just a star), so we
        // must look deeper...
        for (RolapStar.Table child : table.getChildren()) {
            RolapStar.Column rc = child.lookupColumn(factColumnName);
            if (rc != null) {
                return rc;
            } else {
                rc = lookupInChildren(child, factColumnName);
                if (rc != null) {
                    return rc;
                }
            }
        }
        return null;
    }


    // Question: what if foreign key is seen, but there are also level
    // columns - is this at least is a warning.


    /**
     * If everything is ok, issue warning for each aggTable column
     * that has not been identified as a FACT_COLUMN, MEASURE_COLUMN or
     * LEVEL_COLUMN.
     */
    protected void checkUnusedColumns() {
        msgRecorder.pushContextName("Recognizer.checkUnusedColumns");
        for (JdbcSchema.Table.Column aggColumn : aggTable.getColumns()) {
            if (! aggColumn.hasUsage()) {

                String msg = mres.AggUnknownColumn.str(
                    aggTable.getName(),
                    dbFactTable.getName(),
                    aggColumn.getName()
                );
                // This is a fatal error for explicit recognizer
/*
                 if (this instanceof ExplicitRecognizer) {
                    msgRecorder.reportError(msg);
                } else {
                    msgRecorder.reportWarning(msg);
                }

                Make this just a warning
*/
                msgRecorder.reportWarning(msg);
            }
        }
        msgRecorder.popContextName();
    }

    /**
     * Figure out what aggregator should be associated with a column usage.
     * Generally, this aggregator is simply the RolapAggregator returned by
     * calling the getRollup() method of the fact table column's
     * RolapAggregator. But in the case that the fact table column's
     * RolapAggregator is the "Avg" aggregator, then the special
     * RolapAggregator.AvgFromSum is used.
     * <p>
     * Note: this code assumes that the aggregate table does not have an
     * explicit average aggregation column.
     *
     * @param aggUsage
     * @param factAgg
     */
    protected RolapAggregator convertAggregator(
            final JdbcSchema.Table.Column.Usage aggUsage,
            final RolapAggregator factAgg) {

        // NOTE: This assumes that the aggregate table does not have an explicit
        // average column.
        if (factAgg == RolapAggregator.Avg) {
            String columnExpr = getFactCountExpr(aggUsage);
            return new RolapAggregator.AvgFromSum(columnExpr);
        } else if (factAgg == RolapAggregator.DistinctCount) {
            //return RolapAggregator.Count;
            return RolapAggregator.DistinctCount;
        } else {
            return factAgg;
        }
    }

    /**
     * The method chooses a special aggregator for the aggregate table column's
     * usage.
     * <pre>
     * If the fact table column's aggregator was "Avg":
     *   then if the sibling aggregator was "Avg":
     *      the new aggregator is RolapAggregator.AvgFromAvg
     *   else if the sibling aggregator was "Sum":
     *      the new aggregator is RolapAggregator.AvgFromSum
     * else if the fact table column's aggregator was "Sum":
     *   if the sibling aggregator was "Avg":
     *      the new aggregator is RolapAggregator.SumFromAvg
     * </pre>
     * Note that there is no SumFromSum since that is not a special case
     * requiring a special aggregator.
     * <p>
     * if no new aggregator was selected, then the fact table's aggregator
     * rollup aggregator is used.
     *
     * @param aggUsage
     * @param factAgg
     * @param siblingAgg
     */
    protected RolapAggregator convertAggregator(
            final JdbcSchema.Table.Column.Usage aggUsage,
            final RolapAggregator factAgg,
            final RolapAggregator siblingAgg) {

        msgRecorder.pushContextName("Recognizer.convertAggregator");
        RolapAggregator rollupAgg =  null;

        String columnExpr = getFactCountExpr(aggUsage);
        if (factAgg == RolapAggregator.Avg) {
            if (siblingAgg == RolapAggregator.Avg) {
                rollupAgg =  new RolapAggregator.AvgFromAvg(columnExpr);
            } else if (siblingAgg == RolapAggregator.Sum) {
                rollupAgg =  new RolapAggregator.AvgFromSum(columnExpr);
            }
        } else if (factAgg == RolapAggregator.Sum) {
            if (siblingAgg == RolapAggregator.Avg) {
                rollupAgg =  new RolapAggregator.SumFromAvg(columnExpr);
            } else if (siblingAgg instanceof RolapAggregator.AvgFromAvg) {
                // needed for BUG_1541077.testTotalAmount
                rollupAgg =  new RolapAggregator.SumFromAvg(columnExpr);
            }
        }

        if (rollupAgg == null) {
            rollupAgg = (RolapAggregator) factAgg.getRollup();
        }

        if (rollupAgg == null) {
            String msg = mres.NoAggregatorFound.str(
                aggUsage.getSymbolicName(),
                factAgg.getName(),
                siblingAgg.getName());
            msgRecorder.reportError(msg);
        }

        msgRecorder.popContextName();
        return rollupAgg;
    }

    /**
     * Given an aggregate table column usage, find the column name of the
     * table's fact count column usage.
     *
     * @param aggUsage
     * @return
     */
    private String getFactCountExpr(final JdbcSchema.Table.Column.Usage aggUsage) {
        // get the fact count column name.
        JdbcSchema.Table aggTable = aggUsage.getColumn().getTable();

        // iterator over fact count usages - in the end there can be only one!!
        Iterator it =
            aggTable.getColumnUsages(JdbcSchema.FACT_COUNT_COLUMN_USAGE);
        it.hasNext();
        JdbcSchema.Table.Column.Usage usage =
            (JdbcSchema.Table.Column.Usage) it.next();

        // get the columns name
        String factCountColumnName = usage.getColumn().getName();
        String tableName = aggTable.getName();

        // we want the fact count expression
        MondrianDef.Column column =
            new MondrianDef.Column(tableName, factCountColumnName);
        SqlQuery sqlQuery = star.getSqlQuery();
        String columnExpr = column.getExpression(sqlQuery);
        return columnExpr;
    }

    /**
     * Finds all cubes that use this fact table.
     */
    protected List<RolapCube> findCubes() {
        String name = dbFactTable.getName();

        List<RolapCube> list = new ArrayList<RolapCube>();
        RolapSchema schema = star.getSchema();
        for (RolapCube cube : schema.getCubeList()) {
            if (cube.isVirtual()) {
                continue;
            }
            RolapStar cubeStar = cube.getStar();
            String factTableName = cubeStar.getFactTable().getAlias();
            if (name.equals(factTableName)) {
                list.add(cube);
            }
        }
        return list;
    }

    /**
     * Given a {@link mondrian.olap.MondrianDef.Expression}, returns
     * the associated column name.
     *
     * <p>Note: if the {@link mondrian.olap.MondrianDef.Expression} is
     * not a {@link mondrian.olap.MondrianDef.Column} or {@link
     * mondrian.olap.MondrianDef.KeyExpression}, returns null. This
     * will result in an error.
     */
    protected String getColumnName(MondrianDef.Expression expr) {
        msgRecorder.pushContextName("Recognizer.getColumnName");

        try {
            if (expr instanceof MondrianDef.Column) {
                MondrianDef.Column column = (MondrianDef.Column) expr;
                return column.getColumnName();
            } else if (expr instanceof MondrianDef.KeyExpression) {
                MondrianDef.KeyExpression key = (MondrianDef.KeyExpression) expr;
                return key.toString();
            }

            String msg = mres.NoColumnNameFromExpression.str(
                expr.toString());
            msgRecorder.reportError(msg);

            return null;

        } finally {
            msgRecorder.popContextName();
        }
    }
}

// End Recognizer.java
