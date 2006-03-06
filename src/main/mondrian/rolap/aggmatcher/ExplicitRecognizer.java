/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 12 August, 2001
*/

package mondrian.rolap.aggmatcher;

import mondrian.olap.Hierarchy;
import mondrian.recorder.MessageRecorder;
import mondrian.rolap.*;

import java.util.Iterator;

/**
 * This is the Recognizer for the aggregate table descriptions that appear in
 * the catalog schema files; the user explicitly defines the aggregate.
 *
 * @author Richard M. Emberson
 * @version $Id$
 */
class ExplicitRecognizer extends Recognizer {
    private ExplicitRules.TableDef tableDef;

    ExplicitRecognizer(
            final ExplicitRules.TableDef tableDef,
            final RolapStar star,
            final JdbcSchema.Table dbFactTable,
            final JdbcSchema.Table aggTable,
            final MessageRecorder msgRecorder) {
        super(star, dbFactTable, aggTable, msgRecorder);
        this.tableDef = tableDef;
    }

    /**
     * Get the ExplicitRules.TableDef associated with this instance.
     */
    protected ExplicitRules.TableDef getTableDef() {
        return tableDef;
    }

    /**
     * Get the Matcher to be used to match columns to be ignored.
     */
    protected Recognizer.Matcher getIgnoreMatcher() {
        return getTableDef().getIgnoreMatcher();
    }

    /**
     * Get the Matcher to be used to match the column which is the fact count
     * column.
     */
    protected Recognizer.Matcher getFactCountMatcher() {
        return getTableDef().getFactCountMatcher();
    }

    /**
     * Make the measures for this aggregate table.
     * <p>
     * First, iterate through all of the columns in the table.
     * For each column, iterate through all of the tableDef measures, the
     * explicit definitions of a measure.
     * If the table's column name matches the column name in the measure
     * definition, then make a measure.
     * Next, look through all of the fact table column usage measures.
     * For each such measure usage that has a sibling foreign key usage
     * see if the tableDef has a foreign key defined with the same name.
     * If so, then, for free, we can make a measure for the aggregate using
     * its foreign key.
     * <p>
     *
     * @return number of measures created.
     */
    protected int checkMeasures() {
        msgRecorder.pushContextName("ExplicitRecognizer.checkMeasures");
        try {

            int measureColumnCounts = 0;
            // Look at each aggregate table column. For each measure defined,
            // see if the measure's column name equals the column's name.
            // If so, make the aggregate measure usage for that column.
            for (Iterator it = aggTable.getColumns(); it.hasNext(); ) {
                JdbcSchema.Table.Column aggColumn =
                    (JdbcSchema.Table.Column) it.next();

                // if marked as ignore, then do not consider
                if (aggColumn.hasUsage(JdbcSchema.IGNORE_COLUMN_USAGE)) {
                    continue;
                }

                String aggColumnName = aggColumn.getName();

                for (Iterator mit = getTableDef().getMeasures(); mit.hasNext();) {
                    ExplicitRules.TableDef.Measure measure =
                        (ExplicitRules.TableDef.Measure) mit.next();

                    // Column name match is case insensitive
                    if (measure.getColumnName().equalsIgnoreCase(aggColumnName)) {
                        // Ok, got a match, so now make a measue
                        makeMeasure(measure, aggColumn);
                        measureColumnCounts++;
                    }
                }
            }
            // Ok, now look at all of the fact table columns with measure usage
            // that have a sibling foreign key usage. These can be automagically
            // generated for the aggregate table as long as it still has the foreign
            // key.
            for (Iterator it =
                     dbFactTable.getColumnUsages(JdbcSchema.MEASURE_COLUMN_USAGE);
                 it.hasNext(); ) {
                JdbcSchema.Table.Column.Usage factUsage =
                    (JdbcSchema.Table.Column.Usage) it.next();
                JdbcSchema.Table.Column factColumn = factUsage.getColumn();

                if (factColumn.hasUsage(JdbcSchema.FOREIGN_KEY_COLUMN_USAGE)) {
                    // What we've got here is a measure based upon a foreign key
                    String aggFK =
                        getTableDef().getAggregateFK(factColumn.getName());
                    // OK, not a lost dimension
                    if (aggFK != null) {
                        JdbcSchema.Table.Column aggColumn =
                            aggTable.getColumn(aggFK);

                        // Column name match is case insensitive
                        if (aggColumn == null) {
                            aggColumn = aggTable.getColumn(aggFK.toLowerCase());
                        }
                        if (aggColumn == null) {
                            aggColumn = aggTable.getColumn(aggFK.toUpperCase());
                        }

                        if (aggColumn != null) {
                            makeMeasure(factUsage, aggColumn);
                            measureColumnCounts++;
                        }
                    }
                }
            }
            return measureColumnCounts;

        } finally {
            msgRecorder.popContextName();
        }
    }

    /**
     * Make a measure. This makes a measure usage using the Aggregator found in
     * the RolapStar.Measure associated with the ExplicitRules.TableDef.Measure.
     *
     * @param measure
     * @param aggColumn
     */
    protected void makeMeasure(
            final ExplicitRules.TableDef.Measure measure,
            final JdbcSchema.Table.Column aggColumn) {
        RolapStar.Measure rm = measure.getRolapStarMeasure();

        JdbcSchema.Table.Column.Usage aggUsage =
            aggColumn.newUsage(JdbcSchema.MEASURE_COLUMN_USAGE);

        aggUsage.setSymbolicName(measure.getSymbolicName());
        aggUsage.setAggregator(
                convertAggregator(aggUsage, rm.getAggregator()));

        aggUsage.rMeasure = rm;
    }

    /**
     * This creates a foreign key usage.
     *
     * <p> First the column name of the fact usage which is a foreign key is used to
     * search for a foreign key definition in the ExplicitRules.tableDef.
     * If not found, thats ok, it is just a lost dimension.
     * If found, look for a column in the aggregate table with that name and
     * make a foreign key usage.
     */
    protected int matchForeignKey(
            final JdbcSchema.Table.Column.Usage factUsage) {
        JdbcSchema.Table.Column factColumn = factUsage.getColumn();
        String aggFK = getTableDef().getAggregateFK(factColumn.getName());

        // OK, a lost dimension
        if (aggFK == null) {
            return 0;
        }

        int nosMatched = 0;
        for (Iterator aggit = aggTable.getColumns(); aggit.hasNext();) {
            JdbcSchema.Table.Column aggColumn =
                (JdbcSchema.Table.Column) aggit.next();

            // if marked as ignore, then do not consider
            if (aggColumn.hasUsage(JdbcSchema.IGNORE_COLUMN_USAGE)) {
                continue;
            }

            if (aggFK.equals(aggColumn.getName())) {
                makeForeignKey(factUsage, aggColumn, aggFK);
                nosMatched++;
            }
        }
        return nosMatched;

    }

    /**
     * This creates a level usage. A level usage is a column that is used in a
     * collapsed dimension aggregate table.
     *
     * <p> First, iterate through the ExplicitRules.TableDef's level
     * definitions for one with a name equal to the RolapLevel unique name,
     * i.e., [Time].[Quarter].  Now, using the level's column name, search
     * through the aggregate table's columns for one with that name and make a
     * level usage for the column.  Return true if the aggregate table's column
     * was found.
     */
    protected boolean matchLevel(
            final Hierarchy hierarchy,
            final HierarchyUsage hierarchyUsage,
            final RolapLevel rlevel) {
        msgRecorder.pushContextName("ExplicitRecognizer.matchLevel");
        try {

            // Try to match a Level's name against the RolapLevel unique name.
            String levelUniqueName = rlevel.getUniqueName();
            for (Iterator mit = getTableDef().getLevels(); mit.hasNext();) {
                ExplicitRules.TableDef.Level level =
                    (ExplicitRules.TableDef.Level) mit.next();

                if (level.getName().equals(levelUniqueName)) {
                    // Ok, got a match, so now make a measue
                    //makeLevel(level, xxxxolumn);
                    // Now can we find a column in the aggTable that matches the
                    // Level's column
                    String columnName = level.getColumnName();
                    for (Iterator aggit = aggTable.getColumns();
                            aggit.hasNext();) {
                        JdbcSchema.Table.Column aggColumn =
                            (JdbcSchema.Table.Column) aggit.next();
                        if (aggColumn.getName().equals(columnName)) {
                            makeLevel(aggColumn,
                                      hierarchy,
                                      hierarchyUsage,
                                      getColumnName(rlevel.getKeyExp()),
                                      columnName,
                                      rlevel.getName());
                            return true;
                        }
                    }

                }
            }
            return false;

        } finally {
            msgRecorder.popContextName();
        }
    }
}

// End ExplicitRecognizer.java
