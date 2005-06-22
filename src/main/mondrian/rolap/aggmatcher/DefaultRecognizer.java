/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 12 August, 2001
*/

package mondrian.rolap.aggmatcher;

import mondrian.olap.Hierarchy;
import mondrian.olap.MondrianResource;
import mondrian.recorder.MessageRecorder;
import mondrian.rolap.RolapStar;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.HierarchyUsage;

import java.util.Iterator;

/**
 * This is the default Recognizer. It uses the rules found in the file
 * DefaultRules.xml to find aggregate tables and there columns.
 *
 * @author <a>Richard M. Emberson</a>
 * @version
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
     *
     * @return
     */
    DefaultRules getRules() {
        return aggDefault;
    }

    /**
     * Get the Matcher to be used to match columns to be ignored.
     *
     * @return
     */
    protected Recognizer.Matcher getIgnoreMatcher() {
        return new Recognizer.Matcher() {
            public boolean matches(String name) {
                return false;
            }
        };
    }

    /**
     * Get the Matcher to be used to match the column which is the fact count
     * column.
     *
     * @return
     */
    protected Recognizer.Matcher getFactCountMatcher() {
        return getRules().getFactCountMatcher();
    }

    /**
     * Get the Match used to identify columns that are measures.
     *
     * @param factUsage
     * @return
     */
    protected Recognizer.Matcher getMeasureMatcher(
            JdbcSchema.Table.Column.Usage factUsage) {

        String measureName = factUsage.getSymbolicName();
        String measureColumnName = factUsage.getColumn().getName();
        String aggregateName = factUsage.getAggregator().getName();

        Recognizer.Matcher matcher =
            getRules().getMeasureMatcher(measureName,
                                    measureColumnName,
                                    aggregateName);
        return matcher;
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
            int nosOfMeasureColumns = 0;

            for (Iterator it =
                    dbFactTable.getColumnUsages(JdbcSchema.MEASURE_COLUMN_USAGE);
                    it.hasNext(); ) {
                JdbcSchema.Table.Column.Usage factUsage =
                    (JdbcSchema.Table.Column.Usage) it.next();

                Matcher matcher = getMeasureMatcher(factUsage);

                int nosMatched = 0;
                for (Iterator aggit = aggTable.getColumns(); aggit.hasNext();) {
                    JdbcSchema.Table.Column aggColumn =
                        (JdbcSchema.Table.Column) aggit.next();

                    // if marked as ignore, then do not consider
                    if (aggColumn.hasUsage(JdbcSchema.IGNORE_COLUMN_USAGE)) {
                        continue;
                    }

                    if (matcher.matches(aggColumn.getName())) {
                        makeMeasure(factUsage, aggColumn);

                        nosOfMeasureColumns++;
                        nosMatched++;
                    }
                }
                if (nosMatched > 1) {

                    String msg = mres.getAggMultipleMatchingMeasure(
                            msgRecorder.getContext(),
                            aggTable.getName(),
                            dbFactTable.getName(),
                            new Integer(nosMatched),
                            factUsage.getSymbolicName(),
                            factUsage.getColumn().getName(),
                            factUsage.getAggregator().getName()
                        );
                    msgRecorder.reportError(msg);

                    returnValue = false;
                }
            }
            return nosOfMeasureColumns;

        } finally {
            msgRecorder.popContextName();
        }
    }

    /**
     * This creates a foreign key usage.
     * <p>
     * Using the foreign key Matcher with the fact usage's column name the
     * aggregate table's columns are searched for one that matches.
     * For each that matches a foreign key usage is created (thought if more
     * than one is created its is an error which is handled in the calling code.
     * <p>
     *
     * @param factUsage
     * @return
     */
    protected int matchForeignKey(JdbcSchema.Table.Column.Usage factUsage) {
        JdbcSchema.Table.Column factColumn = factUsage.getColumn();

        // search to see if any of the aggTable's columns match
        Recognizer.Matcher matcher =
            getRules().getForeignKeyMatcher(factColumn.getName());

        int nosMatched = 0;
        for (Iterator aggit = aggTable.getColumns(); aggit.hasNext(); ) {
            JdbcSchema.Table.Column aggColumn =
                (JdbcSchema.Table.Column) aggit.next();

            // if marked as ignore, then do not consider
            if (aggColumn.hasUsage(JdbcSchema.IGNORE_COLUMN_USAGE)) {
                continue;
            }

            if (matcher.matches(aggColumn.getName())) {
                makeForeignKey(factUsage, aggColumn, null);
                nosMatched++;
            }
        }
        return nosMatched;
    }

    /**
     * Create level usages.
     * <p>
     * A Matcher is created using the Hierarchy's name, the RolapLevel name,
     * and the column name associated with the RolapLevel's key expression.
     * The aggregate table columns are search for the first match and, if found,
     * a level usage is created for that column and true is returned.
     * <p>
     *
     * @param hierarchy
     * @param hierarchyUsage
     * @param level
     * @return
     */
    protected boolean matchLevel(final Hierarchy hierarchy,
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

            for (Iterator aggit = aggTable.getColumns(); aggit.hasNext(); ) {
                JdbcSchema.Table.Column aggColumn =
                    (JdbcSchema.Table.Column) aggit.next();

                if (matcher.matches(aggColumn.getName())) {
                    makeLevel(aggColumn,
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
