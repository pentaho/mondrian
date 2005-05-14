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

import mondrian.olap.*;
import mondrian.rolap.*;
import mondrian.recorder.MessageRecorder;
import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.regex.Pattern;

/**
 * A class containing a RolapCube's Aggregate tables exclude/include
 * criteria.
 *
 * @author <a>Richard M. Emberson</a>
 * @version
 */
public class ExplicitRules {
    private static final Logger LOGGER = Logger.getLogger(ExplicitRules.class);

    private static final MondrianResource mres = MondrianResource.instance();

    /**
     * Is the given tableName explicitly excluded from consideration as a
     * candidate aggregate table? return true if it is to be excluded and false
     * otherwise.
     *
     * @param tableName
     * @param aggGroups
     * @return
     */
    public static boolean excludeTable(final String tableName,
                                       final List aggGroups) {
        for (Iterator it = aggGroups.iterator(); it.hasNext(); ) {
            ExplicitRules.Group group = (ExplicitRules.Group) it.next();
            if (group.excludeTable(tableName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return the ExplicitRules.TableDef for a tableName that is a candidate
     * aggregate table. If null is returned, then the default rules are used
     * otherwise if not null, then the ExplicitRules.TableDef is used.
     *
     * @param tableName
     * @param aggGroups
     * @return
     */
    public static ExplicitRules.TableDef getIncludeByTableDef(
                                                    final String tableName,
                                                    final List aggGroups) {
        for (Iterator it = aggGroups.iterator(); it.hasNext(); ) {
            ExplicitRules.Group group = (ExplicitRules.Group) it.next();
            ExplicitRules.TableDef tableDef = group.getIncludeByTableDef(tableName);
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
         *
         * @param cube
         * @param xmlCube
         * @return
         */
        public static ExplicitRules.Group make(final RolapCube cube,
                                             final MondrianDef.Cube xmlCube) {
            Group group = new Group(cube);

            MondrianDef.Relation relation = xmlCube.fact;

            if (relation instanceof MondrianDef.Table) {
                MondrianDef.AggExclude[] aggExcludes =
                    ((MondrianDef.Table) relation).getAggExcludes();
                if (aggExcludes != null) {
                    for (int i = 0; i < aggExcludes.length; i++) {
                        ExplicitRules.Exclude exclude =
                            ExplicitRules.make(aggExcludes[i]);
                        group.addExclude(exclude);
                    }
                }
                MondrianDef.AggTable[] aggTables =
                    ((MondrianDef.Table) relation).getAggTables();
                if (aggTables != null) {
                    for (int i = 0; i < aggTables.length; i++) {
                        ExplicitRules.TableDef tableDef =
                            ExplicitRules.TableDef.make(aggTables[i], group);
                        group.addTableDef(tableDef);
                    }
                }
            } else {
                String msg = mres.getCubeRelationNotTable(
                        cube.getName(),
                        relation.getClass().getName()
                    );
                LOGGER.warn(msg);
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("\n"+group);
            }
            return group;
        }

        private final RolapCube cube;
        private List tableDefs;
        private List excludes;

        public Group(final RolapCube cube) {
            this.cube = cube;
            this.excludes = Collections.EMPTY_LIST;
            this.tableDefs = Collections.EMPTY_LIST;
        }

        /**
         * Get the RolapCube associated with this Group.
         *
         * @return
         */
        public RolapCube getCube() {
            return cube;
        }
        /**
         * Get the RolapStar associated with this Group's RolapCube.
         *
         * @return
         */
        public RolapStar getStar() {
            return getCube().getStar();
        }

        /**
         * Get the name of this Group (its the name of its RolapCube).
         *
         * @return
         */
        public String getName() {
            return getCube().getName();
        }

        /**
         * Are there any rules associated with this Group.
         *
         * @return
         */
        public boolean hasRules() {
            return (excludes != Collections.EMPTY_LIST) ||
                (tableDefs != Collections.EMPTY_LIST);
        }

        /**
         * Add an exclude rule.
         *
         * @param exclude
         */
        public void addExclude(final ExplicitRules.Exclude exclude) {
            if (excludes == Collections.EMPTY_LIST) {
                excludes = new ArrayList();
            }
            excludes.add(exclude);
        }

        /**
         * Add a name or pattern (table) rule.
         *
         * @param tableDef
         */
        public void addTableDef(final ExplicitRules.TableDef tableDef) {
            if (tableDefs == Collections.EMPTY_LIST) {
                tableDefs = new ArrayList();
            }
            tableDefs.add(tableDef);
        }

        /**
         * Is the given tableName excluded.
         *
         * @param tableName
         * @return
         */
        public boolean excludeTable(final String tableName) {
            // See if the table is explicitly, by name, excluded
            for (Iterator it = excludes.iterator(); it.hasNext(); ) {
                ExplicitRules.Exclude exclude = (ExplicitRules.Exclude) it.next();
                if (exclude.isExcluded(tableName)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Is the given tableName included either by exact name or by pattern.
         *
         * @param tableName
         * @return
         */
        public ExplicitRules.TableDef getIncludeByTableDef(final String tableName) {
            // An exact match on a NameTableDef takes precedences over a
            // fuzzy match on a PatternTableDef, so
            // first look throught NameTableDef then PatternTableDef
            for (Iterator it = tableDefs.iterator(); it.hasNext(); ) {
                ExplicitRules.TableDef tableDef = (ExplicitRules.TableDef) it.next();
                if (tableDef instanceof NameTableDef) {
                    if (tableDef.matches(tableName)) {
                        return tableDef;
                    }
                }
            }
            for (Iterator it = tableDefs.iterator(); it.hasNext(); ) {
                ExplicitRules.TableDef tableDef = (ExplicitRules.TableDef) it.next();
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
         *
         * @return
         */
        public String getTableName() {
            RolapStar.Table table = getStar().getFactTable();
            MondrianDef.Relation relation = table.getRelation();
            return relation.getAlias();
        }

        /**
         * Get the database schema name associated with this Group's RolapStar's
         * fact table.
         *
         * @return
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
         *
         * @return
         */
        public String getCatalogName() {
            return null;
        }

        /**
         * Validate the content and structure of this Group.
         *
         * @param msgRecorder
         */
        public void validate(final MessageRecorder msgRecorder) {
            msgRecorder.pushContextName(getName());
            try {
                for (Iterator it = this.tableDefs.iterator(); it.hasNext(); ) {
                    ExplicitRules.TableDef tableDef =
                        (ExplicitRules.TableDef) it.next();
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
            Iterator it = this.tableDefs.iterator();
            while (it.hasNext()) {
                ExplicitRules.TableDef tableDef =
                    (ExplicitRules.TableDef) it.next();
                tableDef.print(pw, subsubprefix);
            }
            pw.print(subprefix);
            pw.println("]");

        }

        public class Table {
            private final String tableName;
            private final ExplicitRules.TableDef tableDef;

            private Table(final String tableName,
                          final ExplicitRules.TableDef tableDef) {
                this.tableName = tableName;
                this.tableDef = tableDef;
            }

            /**
             * Get the database name of the table.
             *
             * @return
             */
            public String getTableName() {
                return this.tableName;
            }

            /**
             * Get the TableDef instance.
             *
             * @return
             */
            public ExplicitRules.TableDef getTableDef() {
                return this.tableDef;
            }

            public String toString() {
                StringWriter sw = new StringWriter(256);
                PrintWriter pw = new PrintWriter(sw);
                print(pw, "");
                pw.flush();
                return sw.toString();
            }
            public void validate(final MessageRecorder msgRecorder) {
                msgRecorder.pushContextName(tableName);
                try {
                } finally {
                    msgRecorder.popContextName();
                }
            }
            public void print(final PrintWriter pw, final String prefix) {
                pw.print(prefix);
                pw.println("ExplicitRules.Table:");
                String subprefix = prefix + "  ";

                pw.print(subprefix);
                pw.print("TableName=");
                pw.println(this.tableName);

                pw.print(subprefix);
                pw.print("TableDef.id=");
                pw.println(this.tableDef.getId());
            }
        }

    }
    private static Exclude make(final MondrianDef.AggExclude aggExclude) {
        return (aggExclude.getNameAttribute() != null)
                ? new ExcludeName(aggExclude.getNameAttribute(),
                                  aggExclude.isIgnoreCase())
                : (Exclude) new ExcludePattern(aggExclude.getPattern(),
                                               aggExclude.isIgnoreCase());
    }

    /**
     * Interface of an Exclude type. There are two implementations, one that
     * excludes by exact name match (as an option, ignore case) and the second
     * that matches a regular expression.
     */
    private interface Exclude {
        /**
         * Return true if the tableName is exculed.
         *
         * @param tableName
         * @return
         */
        boolean isExcluded(final String tableName);

        void validate(final MessageRecorder msgRecorder);

        void print(final PrintWriter pw, final String prefix);
    }

    /**
     * This class is an exact name matching Exclude implementation.
     */
    private static class ExcludeName implements Exclude {
        private final String name;
        private final boolean ignoreCase;

        private ExcludeName(final String name, final boolean ignoreCase) {
            this.name = name;
            this.ignoreCase = ignoreCase;
        }

        /**
         * Get the name that is to be matched.
         *
         * @return
         */
        public String getName() {
            return name;
        }

        /**
         * Returns true if the matching can ignore case.
         *
         * @return
         */
        public boolean isIgnoreCase() {
            return ignoreCase;
        }

        /**
         * Return true if the tableName is exculed.
         *
         * @param tableName
         * @return
         */
        public boolean isExcluded(final String tableName) {
            return (this.ignoreCase)
                ? this.name.equals(tableName)
                : this.name.equalsIgnoreCase(tableName);
        }

        /**
         * Validate that the exclude name matches the table pattern.
         *
         * @param msgRecorder
         */
        public void validate(final MessageRecorder msgRecorder) {
            msgRecorder.pushContextName("ExcludeName");
            try {
                String name = getName();
                checkAttributeString(msgRecorder, name, "name");

/*
RME TODO
                // If name does not match the PatternTableDef pattern,
                // then issue warning.
                // Why, because no table with the exclude's name will
                // ever match the pattern, so the exclude is superfluous.
                // This is best effort.
                Pattern pattern = ExplicitRules.PatternTableDef.this.getPattern();
                boolean patternIgnoreCase =
                            ExplicitRules.PatternTableDef.this.isIgnoreCase();
                boolean ignoreCase = isIgnoreCase();

                // If pattern is ignoreCase and name is any case or pattern
                // is not ignoreCase and name is not ignoreCase, then simply
                // see if name matches.
                // Else pattern in not ignoreCase and name is ignoreCase,
                // then pattern could be "AB.*" and name "abc".
                // Here "abc" would name, but not pattern - but who cares
                if (patternIgnoreCase || ! ignoreCase) {
                    if (! pattern.matcher(name).matches()) {
                        msgRecorder.reportWarning(
                            mres.getSuperfluousExludeName(
                                        msgRecorder.getContext(),
                                        name,
                                        pattern.pattern()));
                    }
                }
*/
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

        private ExcludePattern(final String pattern,
                               final boolean ignoreCase) {
            this.pattern = (ignoreCase)
                ? Pattern.compile(pattern, Pattern.CASE_INSENSITIVE)
                : Pattern.compile(pattern);
        }

        /**
         * Return true if the tableName is exculed.
         *
         * @param tableName
         * @return
         */
        public boolean isExcluded(final String tableName) {
            return pattern.matcher(tableName).matches();
        }

        /**
         * Validate that the exclude pattern overlaps with table pattern.
         *
         * @param msgRecorder
         */
        public void validate(final MessageRecorder msgRecorder) {
            msgRecorder.pushContextName("ExcludePattern");
            try {
                checkAttributeString(msgRecorder,
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
         *
         * @param aggTable
         * @param group
         * @return
         */
        static ExplicitRules.TableDef make(final MondrianDef.AggTable aggTable,
                                         final ExplicitRules.Group group) {
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
         *
         * @param tableDef
         * @param aggTable
         */
        private static void add(final ExplicitRules.TableDef tableDef,
                                final MondrianDef.AggTable aggTable) {


            tableDef.setFactCountName(aggTable.getAggFactCount().getColumnName());

            MondrianDef.AggIgnoreColumn[] ignores =
                        aggTable.getAggIgnoreColumns();

            if (ignores != null) {
                for (int i = 0; i < ignores.length; i++) {
                    tableDef.addIgnoreColumnName(ignores[i].getColumnName());
                }
            }

            MondrianDef.AggForeignKey[] fks = aggTable.getAggForeignKeys();
            if (fks != null) {
                for (int i = 0; i < fks.length; i++) {
                    tableDef.addFK(fks[i]);
                }
            }
            MondrianDef.AggMeasure[] measures = aggTable.getAggMeasures();
            if (measures != null) {
                for (int i = 0; i < measures.length; i++) {
                    addTo(tableDef, measures[i]);
                }
            }

            MondrianDef.AggLevel[] levels = aggTable.getAggLevels();
            if (levels != null) {
                for (int i = 0; i < levels.length; i++) {
                    addTo(tableDef, levels[i]);
                }
            }
        }
        private static void addTo(final ExplicitRules.TableDef tableDef,
                                  final MondrianDef.AggLevel aggLevel) {
            addLevelTo(tableDef,
                       aggLevel.getNameAttribute(),
                       aggLevel.getColumnName());
        }
        private static void addTo(final ExplicitRules.TableDef tableDef,
                                  final MondrianDef.AggMeasure aggMeasure) {
            addMeasureTo(tableDef,
                         aggMeasure.getNameAttribute(),
                         aggMeasure.getColumn());
        }


        public static void addLevelTo(final ExplicitRules.TableDef tableDef,
                                      final String name,
                                      final String columnName) {
            Level level = tableDef.new Level(name, columnName);
            tableDef.add(level);
        }
        public static void addMeasureTo(final ExplicitRules.TableDef tableDef,
                                        final String name,
                                        final String column) {
            Measure measure = tableDef.new Measure(name, column);
            tableDef.add(measure);
        }

        /**
         * This class is used to map from a Level's symbolic name,
         * [Time].[Year] to the aggregate table's column name, TIME_YEAR.
         */
        class Level {
            private final String name;
            private final String columnName;
            private RolapLevel rlevel;

            Level(final String name, final String columnName) {
                this.name = name;
                this.columnName = columnName;
            }

            /**
             * Get the symbolic name, the level name.
             *
             * @return
             */
            public String getName() {
                return name;
            }

            /**
             * Get the foreign key column name of the aggregate table.
             *
             * @return
             */
            public String getColumnName() {
                return columnName;
            }

            /**
             * Get the RolapLevel associated with level name.
             *
             * @return
             */
            public RolapLevel getRolapLevel() {
                return rlevel;
            }

            /**
             * Validates a level's name
             *    AggLevel name is a [hierarchy usage name].[level name]
             * Check that is of length 2, starts with a hierarchy and
             * the "level name" exists.
             *
             * @param msgRecorder
             */
            public void validate(final MessageRecorder msgRecorder) {
                msgRecorder.pushContextName("Level");
                try {
                    String name = getName();
                    String columnName = getColumnName();
                    checkAttributeString(msgRecorder, name, "name");
                    checkAttributeString(msgRecorder, columnName, "column");

                    String[] names = Util.explode(name);
                    // must be [hierarchy usage name].[level name]
                    if (names.length != 2) {
                        msgRecorder.reportError(
                            mres.getBadLevelNameFormat(
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
                                    new String[] { names[0] },
                                    false,
                                    Category.Hierarchy);
                            if (hierarchy == null) {
                                msgRecorder.reportError(
                                    mres.getUnknownHierarchyName(
                                        msgRecorder.getContext(),
                                        names[0]));
                            } else {
                                msgRecorder.reportError(
                                    mres.getUnknownLevelName(
                                            msgRecorder.getContext(),
                                            names[0],
                                            names[1]));
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
         * [Measures].[Unit Sales] to the aggregate table's column
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
             *
             * @return
             */
            public String getName() {
                return name;
            }
            /**
             * Get the symbolic name, the measure name, i.e., [Unit Sales].
             *
             * @return
             */
            public String getSymbolicName() {
                return symbolicName;
            }

            /**
             * Get the aggregate table column name.
             *
             * @return
             */
            public String getColumnName() {
                return columnName;
            }

            /**
             * Get the RolapStar.Measure associated with this symbolic name.
             *
             * @return
             */
            public RolapStar.Measure getRolapStarMeasure() {
                return rolapMeasure;
            }

            /**
             * Validates a measure's name
             *    AggMeasure name is a [Measures].[measure name]
             * Check that is of length 2, starts with "Measures" and
             * the "measure name" exists.
             *
             * @param msgRecorder
             */
            public void validate(final MessageRecorder msgRecorder) {
                msgRecorder.pushContextName("Measure");
                try {
                    String name = getName();
                    String column = getColumnName();
                    checkAttributeString(msgRecorder, name, "name");
                    checkAttributeString(msgRecorder, column, "column");

                    String[] names = Util.explode(name);
                    if (names.length != 2) {
                        msgRecorder.reportError(
                            mres.getBadMeasureNameFormat(
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
                            if (! names[0].equals("Measures")) {
                                msgRecorder.reportError(
                                    mres.getBadMeasures(
                                        msgRecorder.getContext(),
                                        names[0]));
                            } else {
                                msgRecorder.reportError(
                                    mres.getUnknownMeasureName(
                                            msgRecorder.getContext(),
                                            names[1]));
                            }
                        }
                        RolapStar star = cube.getStar();
                        rolapMeasure =
                            star.getFactTable().lookupMeasureByName(names[1]);
                        if (rolapMeasure == null) {
                            msgRecorder.reportError(
                                    mres.getBadMeasureName(
                                       msgRecorder.getContext(),
                                       names[1],
                                       cube.getName()));
                        }
                        symbolicName = names[1];
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
        protected List ignoreColumnNames;
        private Map foreignKeyMap;
        private List levels;
        private List measures;

        protected TableDef(final boolean ignoreCase,
                           final ExplicitRules.Group aggGroup) {
            this.id = nextId();
            this.ignoreCase = ignoreCase;
            this.aggGroup = aggGroup;
            this.foreignKeyMap = Collections.EMPTY_MAP;
            this.levels = Collections.EMPTY_LIST;
            this.measures = Collections.EMPTY_LIST;
            this.ignoreColumnNames = Collections.EMPTY_LIST;
        }

        /**
         * TODO: This does not seemed to be used anywhere???
         *
         * @return
         */
        public int getId() {
            return this.id;
        }

        /**
         * Return true if this name/pattern matching ignores case.
         *
         * @return
         */
        public boolean isIgnoreCase() {
            return this.ignoreCase;
        }

        /**
         * Get the RolapStar associated with this cube.
         *
         * @return
         */
        public RolapStar getStar() {
            return getAggGroup().getStar();
        }

        /**
         * Get the Group with which is a part.
         *
         * @return
         */
        public ExplicitRules.Group getAggGroup() {
            return this.aggGroup;
        }

        /**
         * Get the name of the fact count column.
         *
         * @return
         */
        protected String getFactCountName() {
            return factCountName;
        }
        /**
         * Set the name of the fact count column.
         *
         * @param factCountName
         */
        protected void setFactCountName(final String factCountName) {
            this.factCountName = factCountName;
        }

        /**
         * Get an Iterator over all ignore column name entries.
         *
         * @return
         */
        protected Iterator getIgnoreColumnNames() {
            return ignoreColumnNames.iterator();
        }
        /**
         * Get an Iterator over all level mappings.
         *
         * @return
         */
        public Iterator getLevels() {
            return this.levels.iterator();
        }
        /**
         * Get an Iterator over all level mappings.
         *
         * @return
         */
        public Iterator getMeasures() {
            return this.measures.iterator();
        }

        /**
         * Get Matcher for ignore columns.
         *
         * @return
         */
        protected Recognizer.Matcher getIgnoreMatcher() {
            return new Recognizer.Matcher() {
                public boolean matches(final String name) {
                    for (Iterator it =
                            ExplicitRules.TableDef.this.getIgnoreColumnNames();
                            it.hasNext();) {
                        String ignoreName = (String) it.next();
                        if (ignoreName.equals(name)) {
                            return true;
                        }
                    }
                    return false;
                }
            };
        }

        /**
         * Get Matcher for the fact count column.
         *
         * @return
         */
        protected Recognizer.Matcher getFactCountMatcher() {
            return new Recognizer.Matcher() {
                public boolean matches(String name) {
                    return ExplicitRules.TableDef.this.factCountName.equals(name);
                }
            };
        }

        /**
         * Get the RolapCube associated with this mapping.
         *
         * @return
         */
        RolapCube getCube() {
            return aggGroup.getCube();
        }
        /**
         * Check that ALL of the columns in the dbTable have a mapping in in the
         * tableDef.
         * <p>
         * It is an error if there is a column that does not have a mapping.
         *
         * @param star
         * @param dbFactTable
         * @param dbTable
         * @param msgRecorder
         * @return
         */
        public boolean columnsOK(final RolapStar star,
                                 final JdbcSchema.Table dbFactTable,
                                 final JdbcSchema.Table dbTable,
                                 final MessageRecorder msgRecorder) {

            Recognizer cb = new ExplicitRecognizer(this,
                                                   star,
                                                   dbFactTable,
                                                   dbTable,
                                                   msgRecorder);
            return cb.check();
        }

        /**
         * Add the name of an aggregate table column that is to be ignored.
         *
         * @param ignoreName
         */
        protected void addIgnoreColumnName(final String ignoreName) {
            if (this.ignoreColumnNames == Collections.EMPTY_LIST) {
                this.ignoreColumnNames = new ArrayList();
            }
            this.ignoreColumnNames.add(ignoreName);
        }

        /**
         * Add foreign key mapping entry (maps from fact table foreign key
         * column name to aggregate table foreign key column name).
         *
         * @param fk
         */
        protected void addFK(final MondrianDef.AggForeignKey fk) {
            if (this.foreignKeyMap == Collections.EMPTY_MAP) {
                this.foreignKeyMap = new HashMap();
            }
            this.foreignKeyMap.put(fk.getFactFKColumnName(),
                                   fk.getAggregateFKColumnName());
        }

        /**
         * Get the name of the aggregate table's foreign key column that matches
         * the base fact table's foreign key column or return null.
         *
         * @param baseFK
         * @return
         */
        protected String getAggregateFK(final String baseFK) {
            return (String) this.foreignKeyMap.get(baseFK);
        }

        /**
         * Get an Iterator over the foreign key matchers.
         *
         * @return
         */
        protected Iterator getFKEntries() {
            return this.foreignKeyMap.entrySet().iterator();
        }
        /**
         * Add a Level.
         *
         * @param level
         */
        protected void add(final Level level) {
            if (this.levels == Collections.EMPTY_LIST) {
                this.levels = new ArrayList();
            }
            this.levels.add(level);
        }

        /**
         * Add a Measure.
         *
         * @param measure
         */
        protected void add(final Measure measure) {
            if (this.measures == Collections.EMPTY_LIST) {
                this.measures = new ArrayList();
            }
            this.measures.add(measure);
        }

        /**
         * Does the TableDef match a table with name tableName.
         *
         * @param tableName
         * @return
         */
        public abstract boolean matches(final String tableName);

        /**
         * Validate the Levels and Measures, also make sure each definition
         * is different, both name and column.
         *
         * @param msgRecorder
         */
        public void validate(final MessageRecorder msgRecorder) {
            msgRecorder.pushContextName("TableDef");
            try {
                // used to detect duplicates
                Map namesToObjects = new HashMap();
                // used to detect duplicates
                Map columnsToObjects = new HashMap();

                for (Iterator it = levels.iterator(); it.hasNext(); ) {
                    Level level = (Level) it.next();

                    level.validate(msgRecorder);

                    // Is the level name a duplicate
                    if (namesToObjects.containsKey(level.getName())) {
                        msgRecorder.reportError(
                            mres.getDuplicateLevelNames(
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
                            mres.getDuplicateLevelColumnNames(
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
                for (Iterator it = measures.iterator(); it.hasNext(); ) {
                    Measure measure = (Measure) it.next();

                    measure.validate(msgRecorder);

                    if (namesToObjects.containsKey(measure.getName())) {
                        msgRecorder.reportError(
                            mres.getDuplicateMeasureNames(
                                    msgRecorder.getContext(),
                                    measure.getName()));
                        continue;
                    } else {
                        namesToObjects.put(measure.getName(), measure);
                    }

                    if (columnsToObjects.containsKey(measure.getColumnName())) {
                        Object o = columnsToObjects.get(measure.getColumnName());
                        if (o instanceof Measure) {
                            Measure m = (Measure) o;
                            msgRecorder.reportError(
                                mres.getDuplicateMeasureColumnNames(
                                        msgRecorder.getContext(),
                                        measure.getName(),
                                        m.getName(),
                                        measure.getColumnName()));
                        } else {
                            Level l = (Level) o;
                            msgRecorder.reportError(
                                mres.getDuplicateLevelMeasureColumnNames(
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
                for (Iterator it = getFKEntries(); it.hasNext(); ) {
                    Map.Entry e = (Map.Entry) it.next();
                    String baseFKName = (String) e.getKey();
                    String aggFKName = (String) e.getValue();

                    if (namesToObjects.containsKey(baseFKName)) {
                        msgRecorder.reportError(
                                    mres.getDuplicateFactForeignKey(
                                       msgRecorder.getContext(),
                                       baseFKName,
                                       aggFKName));
                    } else {
                        namesToObjects.put(baseFKName, aggFKName);
                    }
                    if (columnsToObjects.containsKey(aggFKName)) {
                        msgRecorder.reportError(
                                    mres.getDuplicateFactForeignKey(
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
                                    mres.getUnknownLeftJoinCondition(
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
            Iterator it = this.levels.iterator();
            while (it.hasNext()) {
                Level level = (Level) it.next();
                level.print(pw, subsubprefix);
            }
            pw.print(subprefix);
            pw.println("]");

            pw.print(subprefix);
            pw.println("Measures: [");
            it = this.measures.iterator();
            while (it.hasNext()) {
                Measure measure = (Measure) it.next();
                measure.print(pw, subsubprefix);
            }
            pw.print(subprefix);
            pw.println("]");
        }
    }
    static class NameTableDef extends ExplicitRules.TableDef {
        /**
         * Make a NameTableDef from the calalog schema.
         *
         * @param aggName
         * @param group
         * @return
         */
        static ExplicitRules.NameTableDef make(final MondrianDef.AggName aggName,
                                             final ExplicitRules.Group group) {
            ExplicitRules.NameTableDef name =
                new ExplicitRules.NameTableDef(aggName.getNameAttribute(),
                                     aggName.isIgnoreCase(),
                                     group);

            ExplicitRules.TableDef.add(name, aggName);

            return name;
        }

        private final String name;
        public NameTableDef(final String name,
                            final boolean ignoreCase,
                            final ExplicitRules.Group group) {
            super(ignoreCase, group);
            this.name = name;
        }

        /**
         * Does the given tableName match this NameTableDef (either exact match
         * or, if set, a case insensitive match).
         *
         * @param tableName
         * @return
         */
        public boolean matches(final String tableName) {
            return (this.ignoreCase)
                ? this.name.equals(tableName)
                : this.name.equalsIgnoreCase(tableName);
        }

        /**
         * Validate name and base class.
         *
         * @param msgRecorder
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
         * Make a PatternTableDef from the calalog schema.
         *
         * @param aggPattern
         * @param group
         * @return
         */
        static ExplicitRules.PatternTableDef make(
                                    final MondrianDef.AggPattern aggPattern,
                                    final ExplicitRules.Group group) {

            ExplicitRules.PatternTableDef pattern =
                new ExplicitRules.PatternTableDef(aggPattern.getPattern(),
                                        aggPattern.isIgnoreCase(),
                                        group);

            MondrianDef.AggExclude[] excludes = aggPattern.getAggExcludes();
            if (excludes != null) {
                for (int i = 0; i < excludes.length; i++) {
                    Exclude exclude = ExplicitRules.make(excludes[i]);
                    pattern.add(exclude);
                }
            }

            ExplicitRules.TableDef.add(pattern, aggPattern);

            return pattern;
        }

        private final Pattern pattern;
        private List excludes;

        public PatternTableDef(final String pattern,
                               final boolean ignoreCase,
                               final ExplicitRules.Group group) {
            super(ignoreCase, group);
            this.pattern = (this.ignoreCase)
                ? Pattern.compile(pattern, Pattern.CASE_INSENSITIVE)
                : Pattern.compile(pattern);
            this.excludes = Collections.EMPTY_LIST;
        }

        /**
         * Get the Pattern.
         *
         * @return
         */
        public Pattern getPattern() {
            return pattern;
        }

        /**
         * Get an Iterator over the list of Excludes.
         *
         * @return
         */
        public Iterator getExcludes() {
            return excludes.iterator();
        }

        /**
         * Add an Exclude.
         *
         * @param exclude
         */
        private void add(final Exclude exclude) {
            if (this.excludes == Collections.EMPTY_LIST) {
                this.excludes = new ArrayList();
            }
            this.excludes.add(exclude);
        }

        /**
         * Return true if the tableName 1) matches the pattern and 2) is not
         * matched by any of the Excludes.
         *
         * @param tableName
         * @return
         */
        public boolean matches(final String tableName) {
            if (! pattern.matcher(tableName).matches()) {
                return false;
            } else {
                for (Iterator it = getExcludes(); it.hasNext(); ) {
                    Exclude exclude = (Exclude) it.next();
                    if (exclude.isExcluded(tableName)) {
                        return false;
                    }
                }
                return true;
            }
        }

        /**
         * Validate excludes and base class.
         *
         * @param msgRecorder
         */
        public void validate(final MessageRecorder msgRecorder) {
            msgRecorder.pushContextName("PatternTableDef");
            try {
                checkAttributeString(msgRecorder, pattern.pattern(), "pattern");

                for (Iterator it = getExcludes(); it.hasNext(); ) {
                    Exclude exclude = (Exclude) it.next();
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
            Iterator it = this.excludes.iterator();
            while (it.hasNext()) {
                Exclude exclude = (Exclude) it.next();
                exclude.print(pw, subsubprefix);
            }
            pw.print(subprefix);
            pw.println("]");

        }
    }

    /**
     * Helper method used to determine if an attribute with name attrName has a
     * non-empty value.
     *
     * @param msgRecorder
     * @param attrValue
     * @param attrName
     */
    private static void checkAttributeString(final MessageRecorder msgRecorder,
                                             final String attrValue,
                                             final String attrName) {
        if (attrValue == null) {
            msgRecorder.reportError(mres.getNullAttributeString(
                    msgRecorder.getContext(),
                    attrName));
        } else if (attrValue.length() == 0) {
            msgRecorder.reportError(mres.getEmptyAttributeString(
                    msgRecorder.getContext(),
                    attrName));
        }
    }


    private ExplicitRules() {}
}
