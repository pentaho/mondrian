/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap.aggmatcher;

import mondrian.olap.Hierarchy;
import mondrian.resource.MondrianResource;
import mondrian.recorder.MessageRecorder;
import mondrian.rolap.RolapStar;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.HierarchyUsage;

import java.util.Iterator;

/**
 * This is the default Recognizer. It uses the rules found in the file
 * DefaultRules.xml to find aggregate tables and there columns.
 *
 * @author Richard M. Emberson
 * @version $Id$
 */
class DefaultRecognizer extends Recognizer {

    private static final MondrianResource mres = MondrianResource.instance();

    private final DefaultRules aggDefault;

    DefaultRecognizer(final DefaultRules aggDefault,
                      final RolapStar star,
                      final JdbcSchema.Table dbFactTable,
                      final JdbcSchema.Table aggTable,
                      final MessageRecorder msgRecorder) {
        super(star, dbFactTable, aggTable, msgRecorder);
        this.aggDefault = aggDefault;
    }

    /**
     * Get the DefaultRules instance associated with this object.
     */
    DefaultRules getRules() {
        return aggDefault;
    }

    /**
     * Get the Matcher to be used to match columns to be ignored.
     */
    protected Recognizer.Matcher getIgnoreMatcher() {
        return getRules().getIgnoreMatcher();
    }

    /**
     * Get the Matcher to be used to match the column which is the fact count
     * column.
     */
    protected Recognizer.Matcher getFactCountMatcher() {
        return getRules().getFactCountMatcher();
    }

    /**
     * Get the Match used to identify columns that are measures.
     */
    protected Recognizer.Matcher getMeasureMatcher(
            JdbcSchema.Table.Column.Usage factUsage) {

        String measureName = factUsage.getSymbolicName();
        String measureColumnName = factUsage.getColumn().getName();
        String aggregateName = factUsage.getAggregator().getName();

        return getRules().getMeasureMatcher(
            measureName,
            measureColumnName,
            aggregateName);
    }

    /**
     * Create measures for an aggregate table.
     * <p>
     * First, iterator through all fact table measure usages.
     * Create a Matcher for each such usage.
     * Iterate through all aggregate table columns.
     * For each column that matches create a measure usage.
     * <p>
     * Per fact table measure usage, at most only one aggregate measure should
     * be created.
     *
     * @return number of measures created.
     */
    protected int checkMeasures() {
        msgRecorder.pushContextName("DefaultRecognizer.checkMeasures");

        try {
            int measureCountCount = 0;

            for (Iterator<JdbcSchema.Table.Column.Usage> it =
                    dbFactTable.getColumnUsages(JdbcSchema.UsageType.MEASURE);
                    it.hasNext(); ) {
                JdbcSchema.Table.Column.Usage factUsage = it.next();

                Matcher matcher = getMeasureMatcher(factUsage);

                int matchCount = 0;
                for (JdbcSchema.Table.Column aggColumn : aggTable.getColumns()) {
                    // if marked as ignore, then do not consider
                    if (aggColumn.hasUsage(JdbcSchema.UsageType.IGNORE)) {
                        continue;
                    }

                    if (matcher.matches(aggColumn.getName())) {
                        makeMeasure(factUsage, aggColumn);

                        measureCountCount++;
                        matchCount++;
                    }
                }

                if (matchCount > 1) {
                    String msg = mres.AggMultipleMatchingMeasure.str(
                        msgRecorder.getContext(),
                        aggTable.getName(),
                        dbFactTable.getName(),
                        matchCount,
                        factUsage.getSymbolicName(),
                        factUsage.getColumn().getName(),
                        factUsage.getAggregator().getName());
                    msgRecorder.reportError(msg);

                    returnValue = false;
                }
            }
            return measureCountCount;

        } finally {
            msgRecorder.popContextName();
        }
    }

    /**
     * This creates a foreign key usage.
     *
     * <p>Using the foreign key Matcher with the fact usage's column name the
     * aggregate table's columns are searched for one that matches.  For each
     * that matches a foreign key usage is created (thought if more than one is
     * created its is an error which is handled in the calling code.
     */
    protected int matchForeignKey(JdbcSchema.Table.Column.Usage factUsage) {
        JdbcSchema.Table.Column factColumn = factUsage.getColumn();

        // search to see if any of the aggTable's columns match
        Recognizer.Matcher matcher =
            getRules().getForeignKeyMatcher(factColumn.getName());

        int matchCount = 0;
        for (JdbcSchema.Table.Column aggColumn : aggTable.getColumns()) {
            // if marked as ignore, then do not consider
            if (aggColumn.hasUsage(JdbcSchema.UsageType.IGNORE)) {
                continue;
            }

            if (matcher.matches(aggColumn.getName())) {
                makeForeignKey(factUsage, aggColumn, null);
                matchCount++;
            }
        }
        return matchCount;
    }

    /**
     * Create level usages.
     *
     * <p> A Matcher is created using the Hierarchy's name, the RolapLevel
     * name, and the column name associated with the RolapLevel's key
     * expression.  The aggregate table columns are search for the first match
     * and, if found, a level usage is created for that column and true is
     * returned.
     */
    protected boolean matchLevel(
            final Hierarchy hierarchy,
            final HierarchyUsage hierarchyUsage,
            final RolapLevel level) {

        msgRecorder.pushContextName("DefaultRecognizer.matchLevel");
        try {

            String usagePrefix = hierarchyUsage.getUsagePrefix();
            String hierName = hierarchy.getName();
            String levelName = level.getName();
            String levelColumnName = getColumnName(level.getKeyExp());

            Recognizer.Matcher matcher = getRules().getLevelMatcher(
                usagePrefix, hierName, levelName, levelColumnName);

            for (JdbcSchema.Table.Column aggColumn : aggTable.getColumns()) {
                if (matcher.matches(aggColumn.getName())) {
                    makeLevel(
                        aggColumn,
                        hierarchy,
                        hierarchyUsage,
                        getColumnName(level.getKeyExp()),
                        getColumnName(level.getKeyExp()),
                        level.getName());
                    return true;
                }
            }
            return false;

        } finally {
            msgRecorder.popContextName();
        }
    }
}

// End DefaultRecognizer.java
