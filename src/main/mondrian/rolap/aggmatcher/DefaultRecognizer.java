/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/

package mondrian.rolap.aggmatcher;

import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.recorder.MessageRecorder;
import mondrian.resource.MondrianResource;
import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.JdbcSchema.Table.Column;
import mondrian.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * This is the default Recognizer. It uses the rules found in the file
 * DefaultRules.xml to find aggregate tables and there columns.
 *
 * @author Richard M. Emberson
 */
class DefaultRecognizer extends Recognizer {

    private static final MondrianResource mres = MondrianResource.instance();

    private final DefaultRules aggDefault;

    DefaultRecognizer(
        final DefaultRules aggDefault,
        final RolapStar star,
        final JdbcSchema.Table dbFactTable,
        final JdbcSchema.Table aggTable,
        final MessageRecorder msgRecorder)
    {
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
        JdbcSchema.Table.Column.Usage factUsage)
    {
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
                it.hasNext();)
            {
                JdbcSchema.Table.Column.Usage factUsage = it.next();

                Matcher matcher = getMeasureMatcher(factUsage);

                int matchCount = 0;
                for (JdbcSchema.Table.Column aggColumn
                    : aggTable.getColumns())
                {
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
     * and, if found, a level usage is created for that column.
     */
    protected void matchLevels(
        final Hierarchy hierarchy,
        final HierarchyUsage hierarchyUsage)
    {
        msgRecorder.pushContextName("DefaultRecognizer.matchLevel");
        try {
            List<Pair<RolapLevel, JdbcSchema.Table.Column>> levelMatches =
                new ArrayList<Pair<RolapLevel, JdbcSchema.Table.Column>>();
            level_loop:
            for (Level level : hierarchy.getLevels()) {
                if (level.isAll()) {
                    continue;
                }
                final RolapLevel rLevel = (RolapLevel) level;

                String usagePrefix = hierarchyUsage.getUsagePrefix();
                String hierName = hierarchy.getName();
                String levelName = rLevel.getName();
                String levelColumnName = getColumnName(rLevel.getKeyExp());

                Recognizer.Matcher matcher = getRules().getLevelMatcher(
                    usagePrefix, hierName, levelName, levelColumnName);

                for (JdbcSchema.Table.Column aggColumn
                    : aggTable.getColumns())
                {
                    if (matcher.matches(aggColumn.getName())) {
                        levelMatches.add(
                            new Pair<RolapLevel,
                                JdbcSchema.Table.Column>(
                                    rLevel, aggColumn));
                        continue level_loop;
                    }
                }
            }
            if (levelMatches.size() == 0) {
                return;
            }
            // Sort the matches by level depth.
            Collections.sort(
                levelMatches,
                new Comparator<Pair<RolapLevel, JdbcSchema.Table.Column>>() {
                    public int compare(
                        Pair<RolapLevel, Column> o1,
                        Pair<RolapLevel, Column> o2)
                    {
                        return
                            Integer.valueOf(o1.left.getDepth()).compareTo(
                                Integer.valueOf(o2.left.getDepth()));
                    }
                });
            // Validate by iterating.
            for (Pair<RolapLevel, JdbcSchema.Table.Column> pair
                : levelMatches)
            {
                boolean collapsed = true;
                if (levelMatches.indexOf(pair) == 0
                    && pair.left.getDepth() > 1)
                {
                    collapsed = false;
                }
                // Fail if the level is not the first match
                // but the one before is not its parent.
                if (levelMatches.indexOf(pair) > 0
                    && pair.left.getDepth() - 1
                        != levelMatches.get(
                            levelMatches.indexOf(pair) - 1).left.getDepth())
                {
                    msgRecorder.reportError(
                        "The aggregate table "
                        + aggTable.getName()
                        + " contains the column "
                        + pair.right.getName()
                        + " which maps to the level "
                        + pair.left.getUniqueName()
                        + " but its parent level is not part of that aggregation.");
                }
                // Fail if the level is non-collapsed but its members
                // are not unique.
                if (!collapsed
                    && !pair.left.isUnique())
                {
                    msgRecorder.reportError(
                        "The aggregate table "
                        + aggTable.getName()
                        + " contains the column "
                        + pair.right.getName()
                        + " which maps to the level "
                        + pair.left.getUniqueName()
                        + " but that level doesn't have unique members and this level is marked as non collapsed.");
                }
            }
            if (msgRecorder.hasErrors()) {
                return;
            }
            // All checks out. Let's create the levels.
            for (Pair<RolapLevel, JdbcSchema.Table.Column> pair
                : levelMatches)
            {
                boolean collapsed = true;
                if (levelMatches.indexOf(pair) == 0
                    && pair.left.getDepth() > 1)
                {
                    collapsed = false;
                }
                makeLevel(
                    pair.right,
                    hierarchy,
                    hierarchyUsage,
                    pair.right.column.name,
                    getColumnName(pair.left.getKeyExp()),
                    pair.left.getName(),
                    collapsed,
                    pair.left);
            }
        } finally {
            msgRecorder.popContextName();
        }
    }
}

// End DefaultRecognizer.java
