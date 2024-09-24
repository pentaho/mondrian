/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.aggmatcher;

import mondrian.olap.*;
import mondrian.recorder.MessageRecorder;
import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.JdbcSchema.Table.Column;
import mondrian.util.Pair;

import java.util.*;

/**
 * This is the Recognizer for the aggregate table descriptions that appear in
 * the catalog schema files; the user explicitly defines the aggregate.
 *
 * @author Richard M. Emberson
 */
class ExplicitRecognizer extends Recognizer {
    private ExplicitRules.TableDef tableDef;
    private RolapCube cube;

    ExplicitRecognizer(
        final ExplicitRules.TableDef tableDef,
        final RolapStar star,
        RolapCube cube,
        final JdbcSchema.Table dbFactTable,
        final JdbcSchema.Table aggTable,
        final MessageRecorder msgRecorder)
    {
        super(star, dbFactTable, aggTable, msgRecorder);
        this.tableDef = tableDef;
        this.cube = cube;
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

    @Override
    protected Matcher getMeasureFactCountMatcher() {
        return  getTableDef().getMeasureFactCountMatcher();
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
            for (JdbcSchema.Table.Column aggColumn : aggTable.getColumns()) {
                // if marked as ignore, then do not consider
                if (aggColumn.hasUsage(JdbcSchema.UsageType.IGNORE)) {
                    continue;
                }

                String aggColumnName = aggColumn.getName();

                for (ExplicitRules.TableDef.Measure measure
                    : getTableDef().getMeasures())
                {
                    // Column name match is case insensitive
                    if (measure.getColumnName().equalsIgnoreCase(aggColumnName))
                    {
                        String name = measure.getName();
                        List<Id.Segment> parts = Util.parseIdentifier(name);
                        Id.Segment nameLast = Util.last(parts);

                        RolapStar.Measure m = null;
                        if (nameLast instanceof Id.NameSegment) {
                            m = star.getFactTable().lookupMeasureByName(
                                cube.getName(),
                                ((Id.NameSegment) nameLast).name);
                        }
                        RolapAggregator agg = null;
                        if (m != null) {
                            agg = m.getAggregator();
                        }
                        // Ok, got a match, so now make a measure
                        makeMeasure(measure, agg, aggColumn);
                        measureColumnCounts++;
                    }
                }
            }
            // Ok, now look at all of the fact table columns with measure usage
            // that have a sibling foreign key usage. These can be automagically
            // generated for the aggregate table as long as it still has the
            // foreign key.
            for (Iterator<JdbcSchema.Table.Column.Usage> it =
                     dbFactTable.getColumnUsages(JdbcSchema.UsageType.MEASURE);
                 it.hasNext();)
            {
                JdbcSchema.Table.Column.Usage factUsage = it.next();
                JdbcSchema.Table.Column factColumn = factUsage.getColumn();

                if (factColumn.hasUsage(JdbcSchema.UsageType.FOREIGN_KEY)) {
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
     */
    protected void makeMeasure(
        final ExplicitRules.TableDef.Measure measure,
        RolapAggregator factAgg,
        final JdbcSchema.Table.Column aggColumn)
    {
        RolapStar.Measure rm = measure.getRolapStarMeasure();

        JdbcSchema.Table.Column.Usage aggUsage =
            aggColumn.newUsage(JdbcSchema.UsageType.MEASURE);

        aggUsage.setSymbolicName(measure.getSymbolicName());

        ExplicitRules.TableDef.RollupType explicitRollupType = measure
                .getExplicitRollupType();
        RolapAggregator ra = null;

        // precedence to the explicitly defined rollup type
        if (explicitRollupType != null) {
            String factCountExpr = getFactCountExpr(aggUsage);
            ra = explicitRollupType.getAggregator(factCountExpr);
        } else {
            ra = (factAgg == null)
                    ? convertAggregator(aggUsage, rm.getAggregator())
                    : convertAggregator(aggUsage, factAgg, rm.getAggregator());
        }

        aggUsage.setAggregator(ra);
        aggUsage.rMeasure = rm;
    }

    /**
     * Creates a foreign key usage.
     *
     * <p> First the column name of the fact usage which is a foreign key is
     * used to search for a foreign key definition in the
     * ExplicitRules.tableDef.  If not found, thats ok, it is just a lost
     * dimension.  If found, look for a column in the aggregate table with that
     * name and make a foreign key usage.
     */
    protected int matchForeignKey(
        final JdbcSchema.Table.Column.Usage factUsage)
    {
        JdbcSchema.Table.Column factColumn = factUsage.getColumn();
        String aggFK = getTableDef().getAggregateFK(factColumn.getName());

        // OK, a lost dimension
        if (aggFK == null) {
            return 0;
        }

        int matchCount = 0;
        for (JdbcSchema.Table.Column aggColumn : aggTable.getColumns()) {
            // if marked as ignore, then do not consider
            if (aggColumn.hasUsage(JdbcSchema.UsageType.IGNORE)) {
                continue;
            }

            if (aggFK.equals(aggColumn.getName())) {
                makeForeignKey(factUsage, aggColumn, aggFK);
                matchCount++;
            }
        }
        return matchCount;
    }

    /**
     * Creates a level usage. A level usage is a column that is used in a
     * collapsed dimension aggregate table.
     *
     * <p> First, iterate through the ExplicitRules.TableDef's level
     * definitions for one with a name equal to the RolapLevel unique name,
     * i.e., [Time].[Quarter].  Now, using the level's column name, search
     * through the aggregate table's columns for one with that name and make a
     * level usage for the column.
     */
    protected void matchLevels(
        final Hierarchy hierarchy,
        final HierarchyUsage hierarchyUsage)
    {
        msgRecorder.pushContextName("ExplicitRecognizer.matchLevel");
        try {
            // Try to match a Level's name against the RolapLevel
            // unique name.
            List<Pair<RolapLevel, ExplicitRules.TableDef.Level>> levelMatches =
                new ArrayList<Pair<RolapLevel, ExplicitRules.TableDef.Level>>();
            List<ExplicitRules.TableDef.Level> aggLevels =
                new ArrayList<ExplicitRules.TableDef.Level>();

            Map<String, Column> aggTableColumnMap =
                getCaseInsensitiveColumnMap();
            Map<String, ExplicitRules.TableDef.Level>
                tableDefLevelUniqueNameMap  = getTableDefLevelUniqueNameMap();

            for (Level hLevel : hierarchy.getLevels()) {
                final RolapLevel rLevel = (RolapLevel) hLevel;
                String levelUniqueName = rLevel.getUniqueName();

                if (tableDefLevelUniqueNameMap.containsKey(levelUniqueName)) {
                    ExplicitRules.TableDef.Level level =
                        tableDefLevelUniqueNameMap.get(levelUniqueName);
                    if (aggTableColumnMap.containsKey(level.getColumnName())) {
                        levelMatches.add(
                            new Pair<RolapLevel, ExplicitRules.TableDef.Level>(
                                rLevel, level));
                        aggLevels.add(level);
                    }
                }
            }
            if (levelMatches.size() == 0) {
                return;
            }
            sortLevelMatches(levelMatches, aggLevels);
            boolean forceCollapse =
                validateLevelMatches(levelMatches, aggLevels);

            if (msgRecorder.hasErrors()) {
                return;
            }
            // All checks out. Let's create the levels.
            for (Pair<RolapLevel, ExplicitRules.TableDef.Level> pair
                : levelMatches)
            {
                RolapLevel rolapLevel = pair.left;
                ExplicitRules.TableDef.Level aggLevel = pair.right;

                makeLevelColumnUsage(
                    aggTableColumnMap.get(aggLevel.getColumnName()),
                    hierarchy,
                    hierarchyUsage,
                    getColumnName(aggLevel.getRolapFieldName()),
                    aggLevels.get(levelMatches.indexOf(pair)).getColumnName(),
                    rolapLevel.getName(),
                    forceCollapse
                        ? true
                        : aggLevels.get(levelMatches.indexOf(pair))
                        .isCollapsed(),
                    rolapLevel,
                    getColumn(aggLevel.getOrdinalColumn(), aggTableColumnMap),
                    getColumn(aggLevel.getCaptionColumn(), aggTableColumnMap),
                    getProperties(aggLevel.getProperties(), aggTableColumnMap));
            }
        } finally {
            msgRecorder.popContextName();
        }
    }

    private Column getColumn(
        String columnName, Map<String, Column> aggTableColumnMap)
    {
        if (columnName == null) {
            return null;
        }
        return aggTableColumnMap.get(columnName);
    }

    private Map<String, Column> getProperties(
        Map<String, String> properties, Map<String, Column> columnMap)
    {
        Map<String, Column> map = new HashMap<String, Column>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            map.put(entry.getKey(), getColumn(entry.getValue(), columnMap));
        }
        return Collections.unmodifiableMap(map);
    }

    private Map<String, ExplicitRules.TableDef.Level>
    getTableDefLevelUniqueNameMap()
    {
        Map<String, ExplicitRules.TableDef.Level> tableDefUniqueNameMap =
            new HashMap<String, ExplicitRules.TableDef.Level>();
        for (ExplicitRules.TableDef.Level level : getTableDef().getLevels()) {
            tableDefUniqueNameMap.put(level.getName(), level);
        }
        return Collections.unmodifiableMap(tableDefUniqueNameMap);
    }

    private Map<String, Column> getCaseInsensitiveColumnMap() {
        Map<String, Column> aggTableColumnMap =
            new TreeMap<String, Column>(String.CASE_INSENSITIVE_ORDER);
        aggTableColumnMap.putAll(aggTable.getColumnMap());
        return Collections.unmodifiableMap(aggTableColumnMap);
    }

    private boolean validateLevelMatches(
        List<Pair<RolapLevel, ExplicitRules.TableDef.Level>> levelMatches,
        List<ExplicitRules.TableDef.Level> aggLevels)
    {
        // Validate by iterating.
        boolean forceCollapse = false;
        for (Pair<RolapLevel, ExplicitRules.TableDef.Level> pair
            : levelMatches)
        {
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
            // Warn if this level is marked as non-collapsed but the level
            // above it is present in this agg table.
            if (levelMatches.indexOf(pair) > 0
                && !aggLevels.get(levelMatches.indexOf(pair)).isCollapsed())
            {
                forceCollapse = true;
                msgRecorder.reportWarning(
                    "The aggregate table " + aggTable.getName()
                    + " contains the column " + pair.right.getName()
                    + " which maps to the level "
                    + pair.left.getUniqueName()
                    + " and is marked as non-collapsed, but its parent column is already present.");
            }
            // Fail if the level is the first, it isn't at the top,
            // but it is marked as collapsed.
            if (levelMatches.indexOf(pair) == 0
                && pair.left.getDepth() > 1
                && aggLevels.get(levelMatches.indexOf(pair)).isCollapsed())
            {
                msgRecorder.reportError(
                    "The aggregate table "
                    + aggTable.getName()
                    + " contains the column "
                    + pair.right.getName()
                    + " which maps to the level "
                    + pair.left.getUniqueName()
                    + " but its parent level is not part of that aggregation and this level is marked as collapsed.");
            }
            // Fail if the level is non-collapsed but its members
            // are not unique.
            if (!aggLevels.get(
                    levelMatches.indexOf(pair)).isCollapsed()
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
        return forceCollapse;
    }

    private void sortLevelMatches(
        List<Pair<RolapLevel, ExplicitRules.TableDef.Level>> levelMatches,
        List<ExplicitRules.TableDef.Level> aggLevels)
    {
        // Sort the matches by level depth.
        Collections.sort(
            levelMatches,
            new Comparator<Pair<RolapLevel, ExplicitRules.TableDef.Level>>() {
                public int compare(
                    Pair<RolapLevel, ExplicitRules.TableDef.Level> o1,
                    Pair<RolapLevel, ExplicitRules.TableDef.Level> o2)
                {
                    return Util.compareIntegers(
                        o1.left.getDepth(),
                        o2.left.getDepth());
                }
            });
        Collections.sort(
            aggLevels,
            new Comparator<ExplicitRules.TableDef.Level>() {
                public int compare(
                    ExplicitRules.TableDef.Level o1,
                    ExplicitRules.TableDef.Level o2)
                {
                    return Util.compareIntegers(
                        o1.getRolapLevel().getDepth(),
                        o2.getRolapLevel().getDepth());
                }
            });
    }

    protected String getFactCountColumnName
            (final JdbcSchema.Table.Column.Usage aggUsage) {
        String measureName = aggUsage.getColumn().getName();
        Map<String, String> measuresFactCount = tableDef.getMeasuresFactCount();
        String factCountColumnName = measuresFactCount.get(measureName);
        if (Util.isEmpty(factCountColumnName)) {
            factCountColumnName = getFactCountColumnName();
        }
        return factCountColumnName;
    }
}

// End ExplicitRecognizer.java
