/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/

package mondrian.rolap.aggmatcher;

import mondrian.olap.*;
import mondrian.recorder.MessageRecorder;
import mondrian.resource.MondrianResource;
import mondrian.rolap.*;

import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.regex.Pattern;

/**
 * A class containing a RolapCube's Aggregate tables exclude/include
 * criteria.
 *
 * @author Richard M. Emberson
 */
public class ExplicitRules {
    private static final Logger LOGGER = Logger.getLogger(ExplicitRules.class);

    private static final MondrianResource mres = MondrianResource.instance();

    /**
     * Returns whether the given is tableName explicitly excluded from
     * consideration as a candidate aggregate table.
     */
    public static boolean excludeTable(
        final String tableName,
        final List<Group> aggGroups)
    {
        for (Group group : aggGroups) {
            if (group.excludeTable(tableName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the {@link TableDef} for a tableName that is a candidate
     * aggregate table. If null is returned, then the default rules are used
     * otherwise if not null, then the ExplicitRules.TableDef is used.
     */
    public static ExplicitRules.TableDef getIncludeByTableDef(
        final String tableName,
        final List<Group> aggGroups)
    {
        for (Group group : aggGroups) {
            TableDef tableDef = group.getIncludeByTableDef(tableName);
            if (tableDef != null) {
                return tableDef;
            }
        }
        return null;
    }

    /**
     * This class forms a collection of aggregate table explicit rules for a
     * given cube.
     *
     */
    public static class Group {

        /**
         * Make an ExplicitRules.Group for a given RolapCube given the
         * MondrianDef.Cube associated with that cube.
         */
        public static ExplicitRules.Group make(
            final RolapCube cube,
            final MondrianDef.Cube xmlCube)
        {
            Group group = new Group(cube);

            MondrianDef.Relation relation = xmlCube.fact;

            if (relation instanceof MondrianDef.Table) {
                MondrianDef.AggExclude[] aggExcludes =
                    ((MondrianDef.Table) relation).getAggExcludes();
                if (aggExcludes != null) {
                    for (MondrianDef.AggExclude aggExclude : aggExcludes) {
                        Exclude exclude =
                            ExplicitRules.make(aggExclude);
                        group.addExclude(exclude);
                    }
                }
                MondrianDef.AggTable[] aggTables =
                    ((MondrianDef.Table) relation).getAggTables();
                if (aggTables != null) {
                    for (MondrianDef.AggTable aggTable : aggTables) {
                        TableDef tableDef = TableDef.make(aggTable, group);
                        group.addTableDef(tableDef);
                    }
                }
            } else {
                LOGGER.warn(
                    mres.CubeRelationNotTable.str(
                        cube.getName(),
                        relation.getClass().getName()));
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(Util.nl + group);
            }
            return group;
        }

        private final RolapCube cube;
        private List<TableDef> tableDefs;
        private List<Exclude> excludes;

        public Group(final RolapCube cube) {
            this.cube = cube;
            this.excludes = Collections.emptyList();
            this.tableDefs = Collections.emptyList();
        }

        /**
         * Get the RolapCube associated with this Group.
         */
        public RolapCube getCube() {
            return cube;
        }

        /**
         * Get the RolapStar associated with this Group's RolapCube.
         */
        public RolapStar getStar() {
            return getCube().getStar();
        }

        /**
         * Get the name of this Group (its the name of its RolapCube).
         */
        public String getName() {
            return getCube().getName();
        }

        /**
         * Are there any rules associated with this Group.
         */
        public boolean hasRules() {
            return
                (excludes != Collections.EMPTY_LIST)
                || (tableDefs != Collections.EMPTY_LIST);
        }

        /**
         * Add an exclude rule.
         */
        public void addExclude(final ExplicitRules.Exclude exclude) {
            if (excludes == Collections.EMPTY_LIST) {
                excludes = new ArrayList<Exclude>();
            }
            excludes.add(exclude);
        }

        /**
         * Add a name or pattern (table) rule.
         */
        public void addTableDef(final ExplicitRules.TableDef tableDef) {
            if (tableDefs == Collections.EMPTY_LIST) {
                tableDefs = new ArrayList<TableDef>();
            }
            tableDefs.add(tableDef);
        }

        /**
         * Returns whether the given tableName is excluded.
         */
        public boolean excludeTable(final String tableName) {
            // See if the table is explicitly, by name, excluded
            for (Exclude exclude : excludes) {
                if (exclude.isExcluded(tableName)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Is the given tableName included either by exact name or by pattern.
         */
        public ExplicitRules.TableDef getIncludeByTableDef(
            final String tableName)
        {
            // An exact match on a NameTableDef takes precedences over a
            // fuzzy match on a PatternTableDef, so
            // first look throught NameTableDef then PatternTableDef
            for (ExplicitRules.TableDef tableDef : tableDefs) {
                if (tableDef instanceof NameTableDef) {
                    if (tableDef.matches(tableName)) {
                        return tableDef;
                    }
                }
            }
            for (ExplicitRules.TableDef tableDef : tableDefs) {
                if (tableDef instanceof PatternTableDef) {
                    if (tableDef.matches(tableName)) {
                        return tableDef;
                    }
                }
            }
            return null;
        }

        /**
         * Get the database table name associated with this Group's RolapStar's
         * fact table.
         */
        public String getTableName() {
            RolapStar.Table table = getStar().getFactTable();
            MondrianDef.Relation relation = table.getRelation();
            return relation.getAlias();
        }

        /**
         * Get the database schema name associated with this Group's RolapStar's
         * fact table.
         */
        public String getSchemaName() {
            String schema = null;

            RolapStar.Table table = getStar().getFactTable();
            MondrianDef.Relation relation = table.getRelation();

            if (relation instanceof MondrianDef.Table) {
                MondrianDef.Table mtable = (MondrianDef.Table) relation;
                schema = mtable.schema;
            }
            return schema;
        }
        /**
         * Get the database catalog name associated with this Group's
         * RolapStar's fact table.
         * Note: this currently this always returns null.
         */
        public String getCatalogName() {
            return null;
        }

        /**
         * Validate the content and structure of this Group.
         */
        public void validate(final MessageRecorder msgRecorder) {
            msgRecorder.pushContextName(getName());
            try {
                for (ExplicitRules.TableDef tableDef : tableDefs) {
                    tableDef.validate(msgRecorder);
                }
            } finally {
                msgRecorder.popContextName();
            }
        }

        public String toString() {
            StringWriter sw = new StringWriter(256);
            PrintWriter pw = new PrintWriter(sw);
            print(pw, "");
            pw.flush();
            return sw.toString();
        }

        public void print(final PrintWriter pw, final String prefix) {
            pw.print(prefix);
            pw.println("ExplicitRules.Group:");
            String subprefix = prefix + "  ";
            String subsubprefix = subprefix + "  ";

            pw.print(subprefix);
            pw.print("name=");
            pw.println(getStar().getFactTable().getRelation());

            pw.print(subprefix);
            pw.println("TableDefs: [");
            for (ExplicitRules.TableDef tableDef : tableDefs) {
                tableDef.print(pw, subsubprefix);
            }
            pw.print(subprefix);
            pw.println("]");
        }
    }

    private static Exclude make(final MondrianDef.AggExclude aggExclude) {
        return (aggExclude.getNameAttribute() != null)
            ? new ExcludeName(
                aggExclude.getNameAttribute(),
                aggExclude.isIgnoreCase())
            : (Exclude) new ExcludePattern(
                aggExclude.getPattern(),
                aggExclude.isIgnoreCase());
    }

    /**
     * Interface of an Exclude type. There are two implementations, one that
     * excludes by exact name match (as an option, ignore case) and the second
     * that matches a regular expression.
     */
    private interface Exclude {
        /**
         * Return true if the tableName is excluded.
         *
         * @param tableName Table name
         * @return whether table name is excluded
         */
        boolean isExcluded(final String tableName);

        /**
         * Validate that the exclude name matches the table pattern.
         *
         * @param msgRecorder Message recorder
         */
        void validate(final MessageRecorder msgRecorder);

        /**
         * Prints this rule to a PrintWriter.
         * @param prefix Line prefix, for indentation
         */
        void print(final PrintWriter pw, final String prefix);
    }

    /**
     * Implementation of Exclude which matches names exactly.
     */
    private static class ExcludeName implements Exclude {
        private final String name;
        private final boolean ignoreCase;

        private ExcludeName(final String name, final boolean ignoreCase) {
            this.name = name;
            this.ignoreCase = ignoreCase;
        }

        /**
         * Returns the name to be matched.
         */
        public String getName() {
            return name;
        }

        /**
         * Returns true if the matching can ignore case.
         */
        public boolean isIgnoreCase() {
            return ignoreCase;
        }

        public boolean isExcluded(final String tableName) {
            return (this.ignoreCase)
                ? this.name.equals(tableName)
                : this.name.equalsIgnoreCase(tableName);
        }

        public void validate(final MessageRecorder msgRecorder) {
            msgRecorder.pushContextName("ExcludeName");
            try {
                String name = getName();
                checkAttributeString(msgRecorder, name, "name");


// RME TODO
//                // If name does not match the PatternTableDef pattern,
//                // then issue warning.
//                // Why, because no table with the exclude's name will
//                // ever match the pattern, so the exclude is superfluous.
//                // This is best effort.
//                Pattern pattern =
//                    ExplicitRules.PatternTableDef.this.getPattern();
//                boolean patternIgnoreCase =
//                    ExplicitRules.PatternTableDef.this.isIgnoreCase();
//                boolean ignoreCase = isIgnoreCase();
//
//                // If pattern is ignoreCase and name is any case or pattern
//                // is not ignoreCase and name is not ignoreCase, then simply
//                // see if name matches.
//                // Else pattern in not ignoreCase and name is ignoreCase,
//                // then pattern could be "AB.*" and name "abc".
//                // Here "abc" would name, but not pattern - but who cares
//                if (patternIgnoreCase || ! ignoreCase) {
//                    if (! pattern.matcher(name).matches()) {
//                        msgRecorder.reportWarning(
//                            mres.getSuperfluousExludeName(
//                                        msgRecorder.getContext(),
//                                        name,
//                                        pattern.pattern()));
//                    }
//                }
            } finally {
                msgRecorder.popContextName();
            }
        }

        public void print(final PrintWriter pw, final String prefix) {
            pw.print(prefix);
            pw.println("ExplicitRules.PatternTableDef.ExcludeName:");

            String subprefix = prefix + "  ";

            pw.print(subprefix);
            pw.print("name=");
            pw.println(this.name);

            pw.print(subprefix);
            pw.print("ignoreCase=");
            pw.println(this.ignoreCase);
        }
    }

    /**
     * This class is a regular expression base name matching Exclude
     * implementation.
     */
    private static class ExcludePattern implements Exclude {
        private final Pattern pattern;

        private ExcludePattern(
            final String pattern,
            final boolean ignoreCase)
        {
            this.pattern = (ignoreCase)
                ? Pattern.compile(pattern, Pattern.CASE_INSENSITIVE)
                : Pattern.compile(pattern);
        }

        public boolean isExcluded(final String tableName) {
            return pattern.matcher(tableName).matches();
        }

        public void validate(final MessageRecorder msgRecorder) {
            msgRecorder.pushContextName("ExcludePattern");
            try {
                checkAttributeString(
                    msgRecorder,
                    pattern.pattern(),
                    "pattern");
                //String context = msgRecorder.getContext();
                // Is there any way to determine if the exclude pattern
                // is never a sub-set of the table pattern.
                // I will have to think about this.
                // Until then, this method is empty.
            } finally {
                msgRecorder.popContextName();
            }
        }

        public void print(final PrintWriter pw, final String prefix) {
            pw.print(prefix);
            pw.println("ExplicitRules.PatternTableDef.ExcludePattern:");

            String subprefix = prefix + "  ";

            pw.print(subprefix);
            pw.print("pattern=");
            pw.print(this.pattern.pattern());
            pw.print(":");
            pw.println(this.pattern.flags());
        }
    }

    /**
     * This is the base class for the exact name based and name pattern based
     * aggregate table mapping definitions. It contains the mappings for the
     * fact count column, optional ignore columns, foreign key mappings,
     * measure column mappings and level column mappings.
     */
    public static abstract class TableDef {

        /**
         * Given a MondrianDef.AggTable instance create a TableDef instance
         * which is either a NameTableDef or PatternTableDef.
         */
        static ExplicitRules.TableDef make(
            final MondrianDef.AggTable aggTable,
            final ExplicitRules.Group group)
        {
            return (aggTable instanceof MondrianDef.AggName)
                ? ExplicitRules.NameTableDef.make(
                    (MondrianDef.AggName) aggTable, group)
                : (ExplicitRules.TableDef)
                ExplicitRules.PatternTableDef.make(
                    (MondrianDef.AggPattern) aggTable, group);
        }

        /**
         * This method extracts information from the MondrianDef.AggTable and
         * places it in the ExplicitRules.TableDef. This code is used for both
         * the NameTableDef and PatternTableDef subclasses of TableDef (it
         * extracts information common to both).
         */
        private static void add(
            final ExplicitRules.TableDef tableDef,
            final MondrianDef.AggTable aggTable)
        {
            if (aggTable.getAggFactCount() != null) {
                tableDef.setFactCountName(
                    aggTable.getAggFactCount().getColumnName());
            }

            MondrianDef.AggIgnoreColumn[] ignores =
                aggTable.getAggIgnoreColumns();

            if (ignores != null) {
                for (MondrianDef.AggIgnoreColumn ignore : ignores) {
                    tableDef.addIgnoreColumnName(ignore.getColumnName());
                }
            }

            MondrianDef.AggForeignKey[] fks = aggTable.getAggForeignKeys();
            if (fks != null) {
                for (MondrianDef.AggForeignKey fk : fks) {
                    tableDef.addFK(fk);
                }
            }
            MondrianDef.AggMeasure[] measures = aggTable.getAggMeasures();
            if (measures != null) {
                for (MondrianDef.AggMeasure measure : measures) {
                    addTo(tableDef, measure);
                }
            }

            MondrianDef.AggLevel[] levels = aggTable.getAggLevels();
            if (levels != null) {
                for (MondrianDef.AggLevel level : levels) {
                    addTo(tableDef, level);
                }
            }
        }

        private static void addTo(
            final ExplicitRules.TableDef tableDef,
            final MondrianDef.AggLevel aggLevel)
        {
            addLevelTo(
                tableDef,
                aggLevel.getNameAttribute(),
                aggLevel.getColumnName(),
                aggLevel.isCollapsed());
        }

        private static void addTo(
            final ExplicitRules.TableDef tableDef,
            final MondrianDef.AggMeasure aggMeasure)
        {
            addMeasureTo(
                tableDef,
                aggMeasure.getNameAttribute(),
                aggMeasure.getColumn());
        }

        public static void addLevelTo(
            final ExplicitRules.TableDef tableDef,
            final String name,
            final String columnName,
            final boolean collapsed)
        {
            Level level = tableDef.new Level(name, columnName, collapsed);
            tableDef.add(level);
        }

        public static void addMeasureTo(
            final ExplicitRules.TableDef tableDef,
            final String name,
            final String column)
        {
            Measure measure = tableDef.new Measure(name, column);
            tableDef.add(measure);
        }

        /**
         * This class is used to map from a Level's symbolic name,
         * [Time]&#46;[Year] to the aggregate table's column name, TIME_YEAR.
         */
        class Level {
            private final String name;
            private final String columnName;
            private final boolean collapsed;
            private RolapLevel rlevel;

            Level(
                final String name,
                final String columnName,
                final boolean collapsed)
            {
                this.name = name;
                this.columnName = columnName;
                this.collapsed = collapsed;
            }

            /**
             * Get the symbolic name, the level name.
             */
            public String getName() {
                return name;
            }

            /**
             * Get the foreign key column name of the aggregate table.
             */
            public String getColumnName() {
                return columnName;
            }

            /**
             * Returns whether this level is collapsed (includes
             * parent levels in the agg table).
             */
            public boolean isCollapsed() {
                return collapsed;
            }

            /**
             * Get the RolapLevel associated with level name.
             */
            public RolapLevel getRolapLevel() {
                return rlevel;
            }

            /**
             * Validates a level's name.
             *
             * <p>The level name must be of the form <code>[hierarchy usage
             * name].[level name]</code>.
             *
             * <p>This method checks that is of length 2, starts with a
             * hierarchy and the "level name" exists.
             */
            public void validate(final MessageRecorder msgRecorder) {
                msgRecorder.pushContextName("Level");
                try {
                    String name = getName();
                    String columnName = getColumnName();
                    checkAttributeString(msgRecorder, name, "name");
                    checkAttributeString(msgRecorder, columnName, "column");

                    List<Id.Segment> names = Util.parseIdentifier(name);
                    // must be [hierarchy usage name].[level name]
                    if (!(names.size() == 2
                        || MondrianProperties.instance().SsasCompatibleNaming
                        .get()
                        && names.size() == 3))
                    {
                        msgRecorder.reportError(
                            mres.BadLevelNameFormat.str(
                                msgRecorder.getContext(),
                                name));
                    } else {
                        RolapCube cube = ExplicitRules.TableDef.this.getCube();
                        SchemaReader schemaReader = cube.getSchemaReader();
                        RolapLevel level =
                            (RolapLevel) schemaReader.lookupCompound(
                                cube,
                                names,
                                false,
                                Category.Level);
                        if (level == null) {
                            Hierarchy hierarchy = (Hierarchy)
                                schemaReader.lookupCompound(
                                    cube,
                                    names.subList(0, 1),
                                    false,
                                    Category.Hierarchy);
                            if (hierarchy == null) {
                                msgRecorder.reportError(
                                    mres.UnknownHierarchyName.str(
                                        msgRecorder.getContext(),
                                        names.get(0).toString()));
                            } else {
                                msgRecorder.reportError(
                                    mres.UnknownLevelName.str(
                                        msgRecorder.getContext(),
                                        names.get(0).toString(),
                                        names.get(1).toString()));
                            }
                        }
                        rlevel = level;
                    }
                } finally {
                    msgRecorder.popContextName();
                }
            }

            public String toString() {
                StringWriter sw = new StringWriter(256);
                PrintWriter pw = new PrintWriter(sw);
                print(pw, "");
                pw.flush();
                return sw.toString();
            }

            public void print(final PrintWriter pw, final String prefix) {
                pw.print(prefix);
                pw.println("Level:");
                String subprefix = prefix + "  ";

                pw.print(subprefix);
                pw.print("name=");
                pw.println(this.name);

                pw.print(subprefix);
                pw.print("columnName=");
                pw.println(this.columnName);
            }
        }

        /**
         * This class is used to map from a measure's symbolic name,
         * [Measures]&amp;#46;[Unit Sales] to the aggregate table's column
         * name, UNIT_SALES_SUM.
         */
        class Measure {
            private final String name;
            private String symbolicName;
            private final String columnName;
            private RolapStar.Measure rolapMeasure;

            Measure(final String name, final String columnName) {
                this.name = name;
                this.columnName = columnName;
            }

            /**
             * Get the symbolic name, the measure name, i.e.,
             * [Measures].[Unit Sales].
             */
            public String getName() {
                return name;
            }

            /**
             * Get the symbolic name, the measure name, i.e., [Unit Sales].
             */
            public String getSymbolicName() {
                return symbolicName;
            }

            /**
             * Get the aggregate table column name.
             */
            public String getColumnName() {
                return columnName;
            }

            /**
             * Get the RolapStar.Measure associated with this symbolic name.
             */
            public RolapStar.Measure getRolapStarMeasure() {
                return rolapMeasure;
            }

            /**
             * Validates a measure's name.
             *
             * <p>The measure name must be of the form
             * <blockquote><code>[Measures].[measure name]</code></blockquote>
             *
             * <p>This method checks that is of length 2, starts
             * with "Measures" and the "measure name" exists.
             */
            public void validate(final MessageRecorder msgRecorder) {
                msgRecorder.pushContextName("Measure");
                try {
                    String name = getName();
                    String column = getColumnName();
                    checkAttributeString(msgRecorder, name, "name");
                    checkAttributeString(msgRecorder, column, "column");

                    List<Id.Segment> names = Util.parseIdentifier(name);
                    if (names.size() != 2) {
                        msgRecorder.reportError(
                            mres.BadMeasureNameFormat.str(
                                msgRecorder.getContext(),
                                name));
                    } else {
                        RolapCube cube = ExplicitRules.TableDef.this.getCube();
                        SchemaReader schemaReader = cube.getSchemaReader();
                        Member member = (Member) schemaReader.lookupCompound(
                            cube,
                            names,
                            false,
                            Category.Member);
                        if (member == null) {
                            if (!(names.get(0) instanceof Id.NameSegment
                                    && ((Id.NameSegment) names.get(0)).name
                                        .equals("Measures")))
                            {
                                msgRecorder.reportError(
                                    mres.BadMeasures.str(
                                        msgRecorder.getContext(),
                                        names.get(0).toString()));
                            } else {
                                msgRecorder.reportError(
                                    mres.UnknownMeasureName.str(
                                        msgRecorder.getContext(),
                                        names.get(1).toString()));
                            }
                        }
                        RolapStar star = cube.getStar();
                        rolapMeasure =
                            names.get(1) instanceof Id.NameSegment
                                ? star.getFactTable().lookupMeasureByName(
                                    cube.getName(),
                                    ((Id.NameSegment) names.get(1)).name)
                                : null;
                        if (rolapMeasure == null) {
                            msgRecorder.reportError(
                                mres.BadMeasureName.str(
                                    msgRecorder.getContext(),
                                    names.get(1).toString(),
                                    cube.getName()));
                        }
                        symbolicName = names.get(1).toString();
                    }
                } finally {
                    msgRecorder.popContextName();
                }
            }

            public String toString() {
                StringWriter sw = new StringWriter(256);
                PrintWriter pw = new PrintWriter(sw);
                print(pw, "");
                pw.flush();
                return sw.toString();
            }

            public void print(final PrintWriter pw, final String prefix) {
                pw.print(prefix);
                pw.println("Measure:");
                String subprefix = prefix + "  ";

                pw.print(subprefix);
                pw.print("name=");
                pw.println(this.name);

                pw.print(subprefix);
                pw.print("column=");
                pw.println(this.columnName);
            }
        }

        private static int idCount = 0;
        private static int nextId() {
            return idCount++;
        }

        protected final int id;
        protected final boolean ignoreCase;
        protected final ExplicitRules.Group aggGroup;
        protected String factCountName;
        protected List<String> ignoreColumnNames;
        private Map<String, String> foreignKeyMap;
        private List<Level> levels;
        private List<Measure> measures;
        protected int approxRowCount = Integer.MIN_VALUE;

        protected TableDef(
            final boolean ignoreCase,
            final ExplicitRules.Group aggGroup)
        {
            this.id = nextId();
            this.ignoreCase = ignoreCase;
            this.aggGroup = aggGroup;
            this.foreignKeyMap = Collections.emptyMap();
            this.levels = Collections.emptyList();
            this.measures = Collections.emptyList();
            this.ignoreColumnNames = Collections.emptyList();
        }

        /**
         * Returns an approximate number of rows in this table.
         * A negative value indicates that no estimate is available.
         * @return An estimated row count, or a negative value if no
         * row count approximation was available.
         */
        public int getApproxRowCount() {
            return approxRowCount;
        }

        /**
         * Return true if this name/pattern matching ignores case.
         */
        public boolean isIgnoreCase() {
            return this.ignoreCase;
        }

        /**
         * Get the RolapStar associated with this cube.
         */
        public RolapStar getStar() {
            return getAggGroup().getStar();
        }

        /**
         * Get the Group with which is a part.
         */
        public ExplicitRules.Group getAggGroup() {
            return this.aggGroup;
        }

        /**
         * Get the name of the fact count column.
         */
        protected String getFactCountName() {
            return factCountName;
        }

        /**
         * Set the name of the fact count column.
         */
        protected void setFactCountName(final String factCountName) {
            this.factCountName = factCountName;
        }

        /**
         * Get an Iterator over all ignore column name entries.
         */
        protected Iterator<String> getIgnoreColumnNames() {
            return ignoreColumnNames.iterator();
        }

        /**
         * Gets all level mappings.
         */
        public List<Level> getLevels() {
            return levels;
        }

        /**
         * Gets all level mappings.
         */
        public List<Measure> getMeasures() {
            return measures;
        }

        /**
         * Get Matcher for ignore columns.
         */
        protected Recognizer.Matcher getIgnoreMatcher() {
            return new Recognizer.Matcher() {
                public boolean matches(final String name) {
                    for (Iterator<String> it =
                            ExplicitRules.TableDef.this.getIgnoreColumnNames();
                        it.hasNext();)
                    {
                        String ignoreName = it.next();
                        if (isIgnoreCase()) {
                            if (ignoreName.equalsIgnoreCase(name)) {
                                return true;
                            }
                        } else {
                            if (ignoreName.equals(name)) {
                                return true;
                            }
                        }
                    }
                    return false;
                }
            };
        }

        /**
         * Get Matcher for the fact count column.
         */
        protected Recognizer.Matcher getFactCountMatcher() {
            return new Recognizer.Matcher() {
                public boolean matches(String name) {
                    // Match is case insensitive
                    final String factCountName = TableDef.this.factCountName;
                    return factCountName != null
                        && factCountName.equalsIgnoreCase(name);
                }
            };
        }

        /**
         * Get the RolapCube associated with this mapping.
         */
        RolapCube getCube() {
            return aggGroup.getCube();
        }

        /**
         * Checks that ALL of the columns in the dbTable have a mapping in the
         * tableDef.
         *
         * <p>It is an error if there is a column that does not have a mapping.
         */
        public boolean columnsOK(
            final RolapStar star,
            final JdbcSchema.Table dbFactTable,
            final JdbcSchema.Table dbTable,
            final MessageRecorder msgRecorder)
        {
            Recognizer cb =
                new ExplicitRecognizer(
                    this, star, getCube(), dbFactTable, dbTable, msgRecorder);
            return cb.check();
        }

        /**
         * Adds the name of an aggregate table column that is to be ignored.
         */
        protected void addIgnoreColumnName(final String ignoreName) {
            if (this.ignoreColumnNames == Collections.EMPTY_LIST) {
                this.ignoreColumnNames = new ArrayList<String>();
            }
            this.ignoreColumnNames.add(ignoreName);
        }

        /**
         * Add foreign key mapping entry (maps from fact table foreign key
         * column name to aggregate table foreign key column name).
         */
        protected void addFK(final MondrianDef.AggForeignKey fk) {
            if (this.foreignKeyMap == Collections.EMPTY_MAP) {
                this.foreignKeyMap = new HashMap<String, String>();
            }
            this.foreignKeyMap.put(
                fk.getFactFKColumnName(),
                fk.getAggregateFKColumnName());
        }

        /**
         * Get the name of the aggregate table's foreign key column that matches
         * the base fact table's foreign key column or return null.
         */
        protected String getAggregateFK(final String baseFK) {
            return this.foreignKeyMap.get(baseFK);
        }

        /**
         * Adds a Level.
         */
        protected void add(final Level level) {
            if (this.levels == Collections.EMPTY_LIST) {
                this.levels = new ArrayList<Level>();
            }
            this.levels.add(level);
        }

        /**
         * Adds a Measure.
         */
        protected void add(final Measure measure) {
            if (this.measures == Collections.EMPTY_LIST) {
                this.measures = new ArrayList<Measure>();
            }
            this.measures.add(measure);
        }

        /**
         * Does the TableDef match a table with name tableName.
         */
        public abstract boolean matches(final String tableName);

        /**
         * Validate the Levels and Measures, also make sure each definition
         * is different, both name and column.
         */
        public void validate(final MessageRecorder msgRecorder) {
            msgRecorder.pushContextName("TableDef");
            try {
                // used to detect duplicates
                Map<String, Object> namesToObjects =
                    new HashMap<String, Object>();
                // used to detect duplicates
                Map<String, Object> columnsToObjects =
                    new HashMap<String, Object>();

                for (Level level : levels) {
                    level.validate(msgRecorder);

                    // Is the level name a duplicate
                    if (namesToObjects.containsKey(level.getName())) {
                        msgRecorder.reportError(
                            mres.DuplicateLevelNames.str(
                                msgRecorder.getContext(),
                                level.getName()));
                    } else {
                        namesToObjects.put(level.getName(), level);
                    }

                    // Is the level foreign key name a duplicate
                    if (columnsToObjects.containsKey(level.getColumnName())) {
                        Level l = (Level)
                            columnsToObjects.get(level.getColumnName());
                        msgRecorder.reportError(
                            mres.DuplicateLevelColumnNames.str(
                                msgRecorder.getContext(),
                                level.getName(),
                                l.getName(),
                                level.getColumnName()));
                    } else {
                        columnsToObjects.put(level.getColumnName(), level);
                    }
                }

                // reset names map, but keep the columns from levels
                namesToObjects.clear();
                for (Measure measure : measures) {
                    measure.validate(msgRecorder);

                    if (namesToObjects.containsKey(measure.getName())) {
                        msgRecorder.reportError(
                            mres.DuplicateMeasureNames.str(
                                msgRecorder.getContext(),
                                measure.getName()));
                        continue;
                    } else {
                        namesToObjects.put(measure.getName(), measure);
                    }

                    if (columnsToObjects.containsKey(measure.getColumnName())) {
                        Object o =
                            columnsToObjects.get(measure.getColumnName());
                        if (o instanceof Measure) {
                            Measure m = (Measure) o;
                            msgRecorder.reportError(
                                mres.DuplicateMeasureColumnNames.str(
                                    msgRecorder.getContext(),
                                    measure.getName(),
                                    m.getName(),
                                    measure.getColumnName()));
                        } else {
                            Level l = (Level) o;
                            msgRecorder.reportError(
                                mres.DuplicateLevelMeasureColumnNames.str(
                                    msgRecorder.getContext(),
                                    l.getName(),
                                    measure.getName(),
                                    measure.getColumnName()));
                        }

                    } else {
                        columnsToObjects.put(measure.getColumnName(), measure);
                    }
                }

                // reset both
                namesToObjects.clear();
                columnsToObjects.clear();

                // Make sure that the base fact table foreign key names match
                // real columns
                RolapStar star = getStar();
                RolapStar.Table factTable = star.getFactTable();
                String tableName = factTable.getAlias();
                for (Map.Entry<String, String> e : foreignKeyMap.entrySet()) {
                    String baseFKName = e.getKey();
                    String aggFKName = e.getValue();

                    if (namesToObjects.containsKey(baseFKName)) {
                        msgRecorder.reportError(
                            mres.DuplicateFactForeignKey.str(
                                msgRecorder.getContext(),
                                baseFKName,
                                aggFKName));
                    } else {
                        namesToObjects.put(baseFKName, aggFKName);
                    }
                    if (columnsToObjects.containsKey(aggFKName)) {
                        msgRecorder.reportError(
                            mres.DuplicateFactForeignKey.str(
                                msgRecorder.getContext(),
                                baseFKName,
                                aggFKName));
                    } else {
                        columnsToObjects.put(aggFKName, baseFKName);
                    }

                    MondrianDef.Column c =
                        new MondrianDef.Column(tableName, baseFKName);
                    if (factTable.findTableWithLeftCondition(c) == null) {
                        msgRecorder.reportError(
                            mres.UnknownLeftJoinCondition.str(
                                msgRecorder.getContext(),
                                tableName,
                                baseFKName));
                    }
                }
            } finally {
                msgRecorder.popContextName();
            }
        }

        public String toString() {
            StringWriter sw = new StringWriter(256);
            PrintWriter pw = new PrintWriter(sw);
            print(pw, "");
            pw.flush();
            return sw.toString();
        }

        public void print(final PrintWriter pw, final String prefix) {
            String subprefix = prefix + "  ";
            String subsubprefix = subprefix + "  ";

            pw.print(subprefix);
            pw.print("id=");
            pw.println(this.id);

            pw.print(subprefix);
            pw.print("ignoreCase=");
            pw.println(this.ignoreCase);

            pw.print(subprefix);
            pw.println("Levels: [");
            for (Level level : this.levels) {
                level.print(pw, subsubprefix);
            }
            pw.print(subprefix);
            pw.println("]");

            pw.print(subprefix);
            pw.println("Measures: [");
            for (Measure measure : this.measures) {
                measure.print(pw, subsubprefix);
            }
            pw.print(subprefix);
            pw.println("]");
        }
    }

    static class NameTableDef extends ExplicitRules.TableDef {
        /**
         * Makes a NameTableDef from the catalog schema.
         */
        static ExplicitRules.NameTableDef make(
            final MondrianDef.AggName aggName,
            final ExplicitRules.Group group)
        {
            ExplicitRules.NameTableDef name =
                new ExplicitRules.NameTableDef(
                    aggName.getNameAttribute(),
                    aggName.getApproxRowCountAttribute(),
                    aggName.isIgnoreCase(),
                    group);

            ExplicitRules.TableDef.add(name, aggName);

            return name;
        }

        private final String name;

        public NameTableDef(
            final String name,
            final String approxRowCount,
            final boolean ignoreCase,
            final ExplicitRules.Group group)
        {
            super(ignoreCase, group);
            this.name = name;
            this.approxRowCount = loadApproxRowCount(approxRowCount);
        }

        private int loadApproxRowCount(String approxRowCount) {
            boolean notNullAndNumeric =
                approxRowCount != null
                    && approxRowCount.matches("^\\d+$");
            if (notNullAndNumeric) {
                return Integer.parseInt(approxRowCount);
            } else {
                // if approxRowCount is not set, return MIN_VALUE to indicate
                return Integer.MIN_VALUE;
            }
        }

        /**
         * Does the given tableName match this NameTableDef (either exact match
         * or, if set, a case insensitive match).
         */
        public boolean matches(final String tableName) {
            return (this.ignoreCase)
                ? this.name.equalsIgnoreCase(tableName)
                : this.name.equals(tableName);
        }

        /**
         * Validate name and base class.
         */
        public void validate(final MessageRecorder msgRecorder) {
            msgRecorder.pushContextName("NameTableDef");
            try {
                checkAttributeString(msgRecorder, name, "name");

                super.validate(msgRecorder);
            } finally {
                msgRecorder.popContextName();
            }
        }

        public void print(final PrintWriter pw, final String prefix) {
            pw.print(prefix);
            pw.println("ExplicitRules.NameTableDef:");
            super.print(pw, prefix);

            String subprefix = prefix + "  ";

            pw.print(subprefix);
            pw.print("name=");
            pw.println(this.name);
        }
    }

    /**
     * This class matches candidate aggregate table name with a pattern.
     */
    public static class PatternTableDef extends ExplicitRules.TableDef {

        /**
         * Make a PatternTableDef from the catalog schema.
         */
        static ExplicitRules.PatternTableDef make(
            final MondrianDef.AggPattern aggPattern,
            final ExplicitRules.Group group)
        {
            ExplicitRules.PatternTableDef pattern =
                new ExplicitRules.PatternTableDef(
                    aggPattern.getPattern(),
                    aggPattern.isIgnoreCase(),
                    group);

            MondrianDef.AggExclude[] excludes = aggPattern.getAggExcludes();
            if (excludes != null) {
                for (MondrianDef.AggExclude exclude1 : excludes) {
                    Exclude exclude = ExplicitRules.make(exclude1);
                    pattern.add(exclude);
                }
            }

            ExplicitRules.TableDef.add(pattern, aggPattern);

            return pattern;
        }

        private final Pattern pattern;
        private List<Exclude> excludes;

        public PatternTableDef(
            final String pattern,
            final boolean ignoreCase,
            final ExplicitRules.Group group)
        {
            super(ignoreCase, group);
            this.pattern = (this.ignoreCase)
                ? Pattern.compile(pattern, Pattern.CASE_INSENSITIVE)
                : Pattern.compile(pattern);
            this.excludes = Collections.emptyList();
        }

        /**
         * Get the Pattern.
         */
        public Pattern getPattern() {
            return pattern;
        }

        /**
         * Get an Iterator over the list of Excludes.
         */
        public List<Exclude> getExcludes() {
            return excludes;
        }

        /**
         * Add an Exclude.
         */
        private void add(final Exclude exclude) {
            if (this.excludes == Collections.EMPTY_LIST) {
                this.excludes = new ArrayList<Exclude>();
            }
            this.excludes.add(exclude);
        }

        /**
         * Return true if the tableName 1) matches the pattern and 2) is not
         * matched by any of the Excludes.
         */
        public boolean matches(final String tableName) {
            if (! pattern.matcher(tableName).matches()) {
                return false;
            } else {
                for (Exclude exclude : getExcludes()) {
                    if (exclude.isExcluded(tableName)) {
                        return false;
                    }
                }
                return true;
            }
        }

        /**
         * Validate excludes and base class.
         */
        public void validate(final MessageRecorder msgRecorder) {
            msgRecorder.pushContextName("PatternTableDef");
            try {
                checkAttributeString(msgRecorder, pattern.pattern(), "pattern");

                for (Exclude exclude : getExcludes()) {
                    exclude.validate(msgRecorder);
                }
                super.validate(msgRecorder);
            } finally {
                msgRecorder.popContextName();
            }
        }

        public void print(final PrintWriter pw, final String prefix) {
            pw.print(prefix);
            pw.println("ExplicitRules.PatternTableDef:");
            super.print(pw, prefix);

            String subprefix = prefix + "  ";
            String subsubprefix = subprefix + "  ";

            pw.print(subprefix);
            pw.print("pattern=");
            pw.print(this.pattern.pattern());
            pw.print(":");
            pw.println(this.pattern.flags());

            pw.print(subprefix);
            pw.println("Excludes: [");
            Iterator<Exclude> it = this.excludes.iterator();
            while (it.hasNext()) {
                Exclude exclude = it.next();
                exclude.print(pw, subsubprefix);
            }
            pw.print(subprefix);
            pw.println("]");
        }
    }

    /**
     * Helper method used to determine if an attribute with name attrName has a
     * non-empty value.
     */
    private static void checkAttributeString(
        final MessageRecorder msgRecorder,
        final String attrValue,
        final String attrName)
    {
        if (attrValue == null) {
            msgRecorder.reportError(mres.NullAttributeString.str(
                msgRecorder.getContext(),
                attrName));
        } else if (attrValue.length() == 0) {
            msgRecorder.reportError(mres.EmptyAttributeString.str(
                msgRecorder.getContext(),
                attrName));
        }
    }


    private ExplicitRules() {
    }
}

// End ExplicitRules.java
