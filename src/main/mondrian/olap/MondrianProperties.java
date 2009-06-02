/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 22 December, 2002
*/
package mondrian.olap;

import org.apache.log4j.Logger;
import org.eigenbase.util.property.*;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.Properties;

/**
 * <code>MondrianProperties</code> contains the properties which determine the
 * behavior of a mondrian instance.
 *
 * <p>There is a method for property valid in a
 * <code>mondrian.properties</code> file. Although it is possible to retrieve
 * properties using the inherited {@link Properties#getProperty(String)}
 * method, we recommend that you use methods in this class.
 *
 * <h2>Note to developers</h2>
 *
 * If you add a property, you must:<ul>
 *
 * <li>Add a property definition to this class</li>
 *
 * <li>Modify the default <code>mondrian.properties</code> file checked into
 * source control, with a description of the property and its default
 * value.</li>
 *
 * <li>Modify the
 * <a target="_top" href="{@docRoot}/../configuration.html#Property_list">
 * Configuration Specification</a>.</li>
 * </ul>
 *
 * <p>Similarly if you update or delete a property.
 *
 * @author jhyde
 * @version $Id$
 * @since 22 December, 2002
 */
public class MondrianProperties extends TriggerableProperties {

    private final PropertySource propertySource;
    private int populateCount;

    private static final Logger LOGGER =
            Logger.getLogger(MondrianProperties.class);

    /**
     * Properties, drawn from {@link System#getProperties}, plus the contents
     * of "mondrian.properties" if it exists. A singleton.
     */
    private static MondrianProperties instance;
    private static final String mondrianDotProperties = "mondrian.properties";

    /**
     * Returns the singleton.
     *
     * @return Singleton instance
     */
    public static synchronized MondrianProperties instance() {
        if (instance == null) {
            instance = new MondrianProperties();
            instance.populate();
        }
        return instance;
    }

    public MondrianProperties() {
        this.propertySource =
            new FilePropertySource(new File(mondrianDotProperties));
    }

    public boolean triggersAreEnabled() {
        return EnableTriggers.get();
    }

    /**
     * Represents a place that properties can be read from, and remembers the
     * timestamp that we last read them.
     */
    public interface PropertySource {
        /**
         * Opens an input stream from the source.
         *
         * <p>Also checks the 'last modified' time, which will determine whether
         * {@link #isStale()} returns true.
         *
         * @return input stream
         */
        InputStream openStream();

        /**
         * Returns true if the source exists and has been modified since last
         * time we called {@link #openStream()}.
         *
         * @return whether source has changed since it was last read
         */
        boolean isStale();

        /**
         * Returns the description of this source, such as a filename or URL.
         *
         * @return description of this PropertySource
         */
        String getDescription();
    }

    /**
     * Implementation of {@link PropertySource} which reads from a
     * {@link File}.
     */
    static class FilePropertySource implements PropertySource {
        private final File file;
        private long lastModified;

        FilePropertySource(File file) {
            this.file = file;
            this.lastModified = 0;
        }

        public InputStream openStream() {
            try {
                this.lastModified = file.lastModified();
                return new FileInputStream(file);
            } catch (FileNotFoundException e) {
                throw Util.newInternal(
                    e,
                    "Error while opening properties file '" + file + "'");
            }
        }

        public boolean isStale() {
            return file.exists() &&
                    file.lastModified() > this.lastModified;
        }

        public String getDescription() {
            return "file=" + file.getAbsolutePath() +
                    " (exists=" + file.exists() + ")";
        }
    }

    /**
     * Implementation of {@link PropertySource} which reads from a {@link URL}.
     */
    static class UrlPropertySource implements PropertySource {
        private final URL url;
        private long lastModified;

        UrlPropertySource(URL url) {
            this.url = url;
        }

        private URLConnection getConnection() {
            try {
                return url.openConnection();
            } catch (IOException e) {
                throw Util.newInternal(
                        e,
                        "Error while opening properties file '" + url + "'");
            }
        }

        public InputStream openStream() {
            try {
                final URLConnection connection = getConnection();
                this.lastModified = connection.getLastModified();
                return connection.getInputStream();
            } catch (IOException e) {
                throw Util.newInternal(
                        e,
                        "Error while opening properties file '" + url + "'");
            }
        }

        public boolean isStale() {
            final long lastModified = getConnection().getLastModified();
            return lastModified > this.lastModified;
        }

        public String getDescription() {
            return url.toExternalForm();
        }
    }

    /**
     * Loads this property set from: the file "$PWD/mondrian.properties" (if it
     * exists); the "mondrian.properties" in the CLASSPATH; and from the system
     * properties.
     */
    public void populate() {
        // Read properties file "mondrian.properties", if it exists. If we have
        // read the file before, only read it if it is newer.
        loadIfStale(propertySource);

        URL url = null;
        File file = new File(mondrianDotProperties);
        if (file.exists() && file.isFile()) {
            // Read properties file "mondrian.properties" from PWD, if it
            // exists.
            try {
                url = file.toURI().toURL();
            } catch (MalformedURLException e) {
                LOGGER.warn(
                    "Mondrian: file '"
                    + file.getAbsolutePath()
                    + "' could not be loaded", e);
            }
        } else {
            // Then try load it from classloader
            url =
                MondrianProperties.class.getClassLoader().getResource(
                    mondrianDotProperties);
        }

        if (url != null) {
            load(new UrlPropertySource(url));
        } else {
            LOGGER.warn(
                "mondrian.properties can't be found under '"
                + new File(".").getAbsolutePath() + "' or classloader");
        }

        // copy in all system properties which start with "mondrian."
        int count = 0;
        for (Enumeration keys = System.getProperties().keys();
             keys.hasMoreElements();) {
            String key = (String) keys.nextElement();
            String value = System.getProperty(key);
            if (key.startsWith("mondrian.")) {
                // NOTE: the super allows us to bybase calling triggers
                // Is this the correct behavior?
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("populate: key=" + key + ", value=" + value);
                }
                super.setProperty(key, value);
                count++;
            }
        }
        if (populateCount++ == 0) {
            LOGGER.info("Mondrian: loaded "
                    + count
                    + " system properties");
        }
    }

    /**
     * Reads properties from a source.
     * If the source does not exist, or has not changed since we last read it,
     * does nothing.
     *
     * @param source Source of properties
     */
    private void loadIfStale(PropertySource source) {
        if (source.isStale()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Mondrian: loading " + source.getDescription());
            }
            load(source);
        }
    }

    /**
     * Tries to load properties from a URL. Does not fail, just prints success
     * or failure to log.
     *
     * @param source Source to read properties from
     */
    private void load(final PropertySource source) {
        try {
            load(source.openStream());
            if (populateCount == 0) {
                LOGGER.info(
                    "Mondrian: properties loaded from '"
                    + source.getDescription()
                    + "'");
            }
        } catch (IOException e) {
            LOGGER.error(
                "Mondrian: error while loading properties "
                + "from '" + source.getDescription() + "' (" + e + ")");
        }
    }

    /**
     * Maximum number of simultaneous queries the system will allow.
     *
     * <p>Oracle fails if you try to run more than the 'processes' parameter in
     * init.ora, typically 150. The throughput of Oracle and other databases
     * will probably reduce long before you get to their limit.</p>
     */
    public transient final IntegerProperty QueryLimit =
        new IntegerProperty(
            this, "mondrian.query.limit", 40);

    /**
     * Property containing a list of JDBC drivers to load automatically.
     * Must be a comma-separated list of class names, and the classes must be
     * on the class path.
     */
    public transient final StringProperty JdbcDrivers =
        new StringProperty(
            this,
            "mondrian.jdbcDrivers",
            "sun.jdbc.odbc.JdbcOdbcDriver,"
            + "org.hsqldb.jdbcDriver,"
            + "oracle.jdbc.OracleDriver,"
            + "com.mysql.jdbc.Driver");

    /**
     * Integer property that, if set to a value greater than zero, limits the
     * maximum size of a result set.
     */
    public transient final IntegerProperty ResultLimit =
        new IntegerProperty(
            this, "mondrian.result.limit", 0);

    /**
     * Property that establishes the amount of chunks for querying cells
     * involving high-cardinality dimensions.
     * Should prime with {@link #ResultLimit mondrian.result.limit}.
     */
    public transient final IntegerProperty HighCardChunkSize =
        new IntegerProperty(this, "mondrian.result.highCardChunkSize", 1);


    // mondrian.test properties

    /**
     * String property that determines which tests are run.
     *
     * <p>This is a regular expression as defined by
     * {@link java.util.regex.Pattern}.
     * If this property is specified, only tests whose names match the pattern
     * in its entirety will be run.</p>
     *
     * @see #TestClass
     */
    public transient final StringProperty TestName =
        new StringProperty(
            this, "mondrian.test.Name", null);

    /**
     * String property that determines which test class to run.
     *
     * <p>This is the name of the class which either implements
     * {@code junit.framework.Test} or has a method
     * {@code public [static] junit.framework.Test suite()}.</p>
     *
     * <p>Example:
     * <blockquote><code>
     * mondrian.test.Class=mondrian.test.FoodMartTestCase
     * </code></blockquote>
     *
     * @see #TestName
     */
    public transient final StringProperty TestClass =
        new StringProperty(
            this, "mondrian.test.Class", null);

    /**
     * Property containing the connect string which regresssion tests should
     * use to connect to the database.
     * Format is specified in {@link Util#parseConnectString(String)}.
     */
    public transient final StringProperty TestConnectString =
        new StringProperty(
            this, "mondrian.test.connectString", null);
    /**
     * Property containing a list of dimensions in the Sales cube that should
     * be treated as high-cardinality dimensions by the testing infrastructure.
     * This allows us to run the full suite of tests with high-cardinality
     * functionality enabled.
     */
    public transient final StringProperty TestHighCardinalityDimensionList =
        new StringProperty(
            this, "mondrian.test.highCardDimensions", null);

    // miscellaneous

    /**
     * Property containing the JDBC URL of the FoodMart database.
     * The default value is to connect to an ODBC data source called
     * "MondrianFoodMart".
     */
    public transient final StringProperty FoodmartJdbcURL =
        new StringProperty(
            this, "mondrian.foodmart.jdbcURL", "jdbc:odbc:MondrianFoodMart");

    /**
     * Property containing the JDBC URL of a test database.
     * It does not default.
     */
    public transient final StringProperty TestJdbcURL =
        new StringProperty(
            this, "mondrian.test.jdbcURL", null);

    /**
     * Property containing the JDBC user of a test database.
     * The default value is null, to cope with DBMSs that don't need this.
     */
    public transient final StringProperty TestJdbcUser =
        new StringProperty(
            this, "mondrian.test.jdbcUser", null);

    /**
     * Property containing the JDBC password of a test database.
     * The default value is null, to cope with DBMSs that don't need this.
     */
    public transient final StringProperty TestJdbcPassword =
        new StringProperty(
            this, "mondrian.test.jdbcPassword", null);

    /**
     * Property that, with {@link #SparseSegmentDensityThreshold}, determines
     * whether to choose a sparse or dense representation when storing
     * collections of cell values in memory.
     *
     * <p>When storing collections of cell values, Mondrian has to choose
     * between a sparse and a dense representation, based upon the
     * <code>possible</code> and <code>actual</code> number of values.
     * The <code>density</code> is <code>actual / possible</code>.
     *
     * <p>We use a sparse representation if
     * <code>(possible -
     * {@link #SparseSegmentCountThreshold countThreshold}) *
     * {@link #SparseSegmentDensityThreshold densityThreshold} &gt;
     * actual</code>
     *
     * <p>For example, at the default values
     * ({@link #SparseSegmentCountThreshold countThreshold} = 1000,
     * {@link #SparseSegmentDensityThreshold} = 0.5),
     * we use a dense representation for<ul>
     * <li>(1000 possible, 0 actual), or
     * <li>(2000 possible, 500 actual), or
     * <li>(3000 possible, 1000 actual).
     * </ul>
     * Any fewer actual values, or any more
     * possible values, and Mondrian will use a sparse representation.
     */
    public transient final IntegerProperty SparseSegmentCountThreshold =
        new IntegerProperty(
            this, "mondrian.rolap.SparseSegmentValueThreshold", 1000);

    /**
     * Property that, with {@link #SparseSegmentCountThreshold},
     * determines whether to choose a sparse or dense representation when
     * storing collections of cell values in memory.
     */
    public transient final DoubleProperty SparseSegmentDensityThreshold =
        new DoubleProperty(
            this, "mondrian.rolap.SparseSegmentDensityThreshold", 0.5);

    /**
     * Property that defines
     * a pattern for which test XML files to run.  Pattern has to
     * match a file name of the form:
     * <code>query<i>whatever</i>.xml</code> in the directory.
     *
     * <p>Example:
     * <blockquote><code>
     * mondrian.test.QueryFilePattern=queryTest_fec[A-Za-z0-9_]*.xml
     * </code></blockquote>
     */
    public transient final StringProperty QueryFilePattern =
        new StringProperty(
            this, "mondrian.test.QueryFilePattern", null);

    /**
     * Property defining
     * where the test XML files are.
     */
    public transient final StringProperty QueryFileDirectory =
        new StringProperty(
            this, "mondrian.test.QueryFileDirectory", null);

    /**
     * todo:
     */
    public transient final IntegerProperty Iterations =
        new IntegerProperty(
            this, "mondrian.test.Iterations", 1);

    /**
     * todo:
     */
    public transient final IntegerProperty VUsers =
        new IntegerProperty(
            this, "mondrian.test.VUsers", 1);

    /**
     * Property that returns the time limit for the test run in seconds.
     * If the test is running after that time, it is terminated.
     */
    public transient final IntegerProperty TimeLimit =
        new IntegerProperty(
            this, "mondrian.test.TimeLimit", 0);

    /**
     * Property that indicates whether this is a "warmup test".
     */
    public transient final BooleanProperty Warmup =
        new BooleanProperty(
            this, "mondrian.test.Warmup", false);

    /**
     * Property that contains the URL of the catalog to be used by
     * {@link mondrian.tui.CmdRunner} and XML/A Test.
     */
    public transient final StringProperty CatalogURL =
        new StringProperty(
            this, "mondrian.catalogURL", null);

    /**
     * Property that controls
     * whether aggregation cache hit / miss counters will be enabled
     */
    public transient final BooleanProperty EnableCacheHitCounters =
        new BooleanProperty(
            this, "mondrian.rolap.agg.enableCacheHitCounters", false);

    /**
     * Property that controls if warning messages should be printed if a sql
     * comparison tests do not contain expected sqls for the specified
     * dialect. The tests are skipped if no expected sqls are
     * found for the current dialect.
     *
     * Possible values are the following:
     * "NONE": no warning (default)
     * "ANY": any dialect
     * "ACCESS"
     * "DERBY"
     * "LUCIDDB"
     * "MYSQL"
     *  ...and any Dialect enum in SqlPattern.Dialect
     *
     * Specific tests can overwrite the default setting. The priority is
     * Settings besides "ANY" in mondrian.properties file < Any setting in the test < "ANY"
     *
     */
    public transient final StringProperty WarnIfNoPatternForDialect =
        new StringProperty(
            this, "mondrian.test.WarnIfNoPatternForDialect", "NONE");

    //////////////////////////////////////////////////////////////////////////
    //
    // properties relating to aggregates
    //

    /**
     * Boolean property that controls whether Mondrian uses aggregate tables.
     *
     * <p>If true, then Mondrian uses aggregate tables. This property is
     * queried prior to each aggregate query so that changing the value of this
     * property dynamically (not just at startup) is meaningful.
     *
     * <p>Aggregates can be read from the database using the
     * {@link #ReadAggregates} property but will not be used unless this
     * property is set to true.
     */
    public transient final BooleanProperty UseAggregates =
        new BooleanProperty(
            this, "mondrian.rolap.aggregates.Use", false);

    /**
     * Boolean property that determines whether Mondrian should read aggregate
     * tables.
     *
     * <p>If set to true, then Mondrian scans the database for aggregate tables.
     * Unless mondrian.rolap.aggregates.Use is set to true, the aggregates
     * found will not be used.
     */
    public transient final BooleanProperty ReadAggregates =
        new BooleanProperty(
            this, "mondrian.rolap.aggregates.Read", false);


    /**
     * Boolean property that controls whether aggregate tables
     * are ordered by their volume or row count.
     *
     * <p>If true, Mondrian uses the aggregate table with the smallest volume
     * (number of rows multiplied by number of columns); if false, Mondrian
     * uses the aggregate table with the fewest rows.
     */
    public transient final BooleanProperty ChooseAggregateByVolume =
        new BooleanProperty(
            this, "mondrian.rolap.aggregates.ChooseByVolume", false);

    /**
     * String property containing the name of the file which defines the rules
     * for recognizing an aggregate table. Can be either a resource in the
     * Mondrian jar or a URL.
     *
     * <p>The default value is "/DefaultRules.xml", which is in the
     * mondrian.rolap.aggmatcher package in Mondrian.jar.
     *
     * <p>Normally, this property is not set by a user.
     */
    public transient final StringProperty AggregateRules =
        new StringProperty(
            this, "mondrian.rolap.aggregates.rules", "/DefaultRules.xml");

    /**
     * String property that is the AggRule element's tag value.
     *
     * <p>Normally, this property is not set by a user.
     */
    public transient final StringProperty AggregateRuleTag =
        new StringProperty(
            this, "mondrian.rolap.aggregates.rule.tag", "default");

    /**
     * Boolean property that controls whether to print the SQL code
     * generated for aggregate tables.
     *
     * <p>If set, then as each aggregate request is processed, both the lost
     * and collapsed dimension create and insert sql code is printed.
     * This is for use in the CmdRunner allowing one to create aggregate table
     * generation sql.
     */
    public transient final BooleanProperty GenerateAggregateSql =
        new BooleanProperty(
            this, "mondrian.rolap.aggregates.generateSql", false);

    //
    //////////////////////////////////////////////////////////////////////////

    /**
     * Boolean property that controls whether a RolapStar's
     * aggregate data cache is cleared after each query.
     * If true, no RolapStar will cache aggregate data from one
     * query to the next (the cache is cleared after each query).
     */
    public transient final BooleanProperty DisableCaching =
        new BooleanProperty(
            this, "mondrian.rolap.star.disableCaching", false);

    /**
     * Boolean property that controls whether to notify the Mondrian system
     * when a {@link MondrianProperties property value} changes.
     *
     * <p>This allows objects dependent on Mondrian properties to react (that
     * is, reload), when a given property changes via, say,
     * <code>MondrianProperties.instance().populate(null)</code> or
     * <code>MondrianProperties.instance().QueryLimit.set(50)</code>.
     */
    public transient final BooleanProperty EnableTriggers =
        new BooleanProperty(
            this, "mondrian.olap.triggers.enable", true);

    /**
     * Boolean property that controls pretty-print mode.
     * If set to true, the all SqlQuery SQL strings
     * will be generated in pretty-print mode, formatted for ease of reading.
     */
    public transient final BooleanProperty GenerateFormattedSql =
        new BooleanProperty(
            this, "mondrian.rolap.generate.formatted.sql", false);

    /**
     * Boolean property that controls whether each query axis implicit has the
     * NON EMPTY option set. The default is false.
     */
    public transient final BooleanProperty EnableNonEmptyOnAllAxis =
        new BooleanProperty(
            this, "mondrian.rolap.nonempty", false);

    /**
     * When looking for native evaluation of an expression, expand non native
     * subexpressions into MemberLists.
     */
    public transient final BooleanProperty ExpandNonNative =
        new BooleanProperty(
            this, "mondrian.native.ExpandNonNative", false);

    /**
     * Boolean property that controls whether sibling members are
     * compared according to order key value fetched from their ordinal
     * expression.  The default is false (only database ORDER BY is used).
     */
    public transient final BooleanProperty CompareSiblingsByOrderKey =
        new BooleanProperty(
            this, "mondrian.rolap.compareSiblingsByOrderKey", false);

    /**
     * Boolean property that controls whether to use a cache for frequently
     * evaluated expressions. With the cache disabled, an expression like
     * <code>Rank([Product].CurrentMember,
     * Order([Product].MEMBERS, [Measures].[Unit Sales]))</code> would perform
     * many redundant sorts. The default is true.
     */
    public transient final BooleanProperty EnableExpCache =
        new BooleanProperty(
            this, "mondrian.expCache.enable", true);

    /**
     * Integer property that controls whether to test operators' dependencies,
     * and how much time to spend doing it.
     *
     * <p>If this property is positive, Mondrian's test framework allocates an
     * expression evaluator which evaluates each expression several times, and
     * makes sure that the results of the expression are independent of
     * dimensions which the expression claims to be independent of.
     *
     * <p>The default is 0.
     */
    public transient final IntegerProperty TestExpDependencies =
        new IntegerProperty(
            this, "mondrian.test.ExpDependencies", 0);

    /**
     * Seed for random number generator used by some of the tests.
     *
     * <p>Any value besides 0 or -1 gives deterministic behavior.
     * The default value is 1234: most users should use this.
     * Setting the seed to a different value can increase coverage, and
     * therefore may uncover new bugs.
     *
     * <p>If you set the value to 0, the system will generate its own
     * pseudo-random seed.
     *
     * <p>If you set the value to -1, Mondrian uses the next seed from an
     * internal random-number generator. This is a little more deterministic
     * than setting the value to 0.
     */
    public transient final IntegerProperty TestSeed =
        new IntegerProperty(
            this, "mondrian.test.random.seed", 1234);

    /**
     * String property that holds the
     * name of the class whose resource bundle is to be used to for this
     * schema. For example, if the class is {@code com.acme.MyResource},
     * mondrian will look for a resource bundle called
     * {@code com/acme/MyResource_<i>locale</i>.properties} on the class path.
     * (This property has a confusing name because in a previous release it
     * actually held a file name.)
     *
     * <p>Used for the {@link mondrian.i18n.LocalizingDynamicSchemaProcessor};
     * see <a href="{@docRoot}/../schema.html#I18n">Internationalization</a>
     * for more details.</td>
     *
     * <p>Default value is null.
     */
    public transient final StringProperty LocalePropFile =
        new StringProperty(
            this, "mondrian.rolap.localePropFile", null);

    /**
     * if enabled some NON EMPTY CrossJoin will be computed in SQL
     */
    public transient final BooleanProperty EnableNativeCrossJoin =
        new BooleanProperty(
            this, "mondrian.native.crossjoin.enable", true);

    /**
     * if enabled some TopCount will be computed in SQL
     */
    public transient final BooleanProperty EnableNativeTopCount =
        new BooleanProperty(
            this, "mondrian.native.topcount.enable", true);

    /**
     * if enabled some Filter() will be computed in SQL
     */
    public transient final BooleanProperty EnableNativeFilter =
        new BooleanProperty(
            this, "mondrian.native.filter.enable", true);

    /**
     * some NON EMPTY set operations like member.children, level.members and
     * member descendants will be computed in SQL
     */
    public transient final BooleanProperty EnableNativeNonEmpty =
        new BooleanProperty(
            this, "mondrian.native.nonempty.enable", true);

    /**
     * Alerting action to take in case native evaluation of a function is
     * enabled but not supported for that function's usage in a particular
     * query.  (No alert is ever raised in cases where native evaluation would
     * definitely have been wasted effort.)
     *
     *
     *
     * Recognized actions:
     *
     * <ul>
     *
     * <li><code>OFF</code>:  do nothing (default action, also used if
     * unrecognized action is specified)
     *
     * <li><code>WARN</code>:  log a warning to RolapUtil logger
     *
     * <li><code>ERROR</code>:  throw an instance of
     * {@link NativeEvaluationUnsupportedException}
     *
     * </ul>
     */
    public transient final StringProperty AlertNativeEvaluationUnsupported =
        new StringProperty(this, "mondrian.native.unsupported.alert", "OFF");

    /**
     * If enabled, first row in the result of an XML/A drill-through request
     * will be filled with the total count of rows in underlying database.
     */
    public transient final BooleanProperty EnableTotalCount =
        new BooleanProperty(
            this, "mondrian.xmla.drillthroughTotalCount.enable", true);

    /**
     * Boolean property that controls whether the MDX parser resolves uses
     * case-sensitive matching when looking up identifiers. The default is
     * false.
     */
    public transient final BooleanProperty CaseSensitive = new BooleanProperty(
        this, "mondrian.olap.case.sensitive", false);


    /**
     * Property that defines
     * limit on the number of rows returned by XML/A drill through request.
     */
    public transient final IntegerProperty MaxRows =
        new IntegerProperty(
            this, "mondrian.xmla.drillthroughMaxRows", 1000);

    /**
     * Max number of constraints in a single `IN' SQL clause.
     *
     * <p>This value may be variant among database prodcuts and their runtime
     * settings. Oracle, for example, gives the error "ORA-01795: maximum
     * number of expressions in a list is 1000".
     *
     * <p>Recommended values:<ul>
     * <li>Oracle: 1,000
     * <li>DB2: 2,500
     * <li>Other: 10,000</ul>
     */
    public transient final IntegerProperty MaxConstraints =
        new IntegerProperty(
            this, "mondrian.rolap.maxConstraints", 1000);

    /**
     * Boolean property that determines whether Mondrian optimizes predicates.
     */
    public transient final BooleanProperty OptimizePredicates =
        new BooleanProperty(
            this, "mondrian.rolap.aggregates.optimizePredicates", true);

    /**
     * Boolean property that defines the
     * maximum number of passes allowable while evaluating an MDX expression.
     *
     * <p>If evaluation exceeds this depth (for example, while evaluating a
     * very complex calculated member), Mondrian will throw an error.
     */
    public transient final IntegerProperty MaxEvalDepth =
        new IntegerProperty(
            this, "mondrian.rolap.evaluate.MaxEvalDepth", 10);

    /**
     * Property that defines the JdbcSchema factory class which
     * determines the list of tables and columns of a specific datasource.
     * @see mondrian.rolap.aggmatcher.JdbcSchema
     */
    public transient final StringProperty JdbcFactoryClass =
        new StringProperty(
            this, "mondrian.rolap.aggregates.jdbcFactoryClass", null);

    /**
     * Property that defines
     * the timeout value (in seconds) for queries; 0, the default, indicates no
     * timeout.
     */
    public transient final IntegerProperty QueryTimeout = new IntegerProperty(
        this, "mondrian.rolap.queryTimeout", 0);

    /**
     * Property that defines
     * whether non-existent member errors should be ignored during schema
     * load.
     */
    public transient final BooleanProperty IgnoreInvalidMembers =
        new BooleanProperty(
            this, "mondrian.rolap.ignoreInvalidMembers", false);

    /**
     * Property that defines
     * whether non-existent member errors should be ignored during query
     * validation.
     */
    public transient final BooleanProperty IgnoreInvalidMembersDuringQuery =
        new BooleanProperty(
            this, "mondrian.rolap.ignoreInvalidMembersDuringQuery", false);

    /**
     * Property that determines how a null member value is represented in the
     * result output.
     * <p>AS 2000 shows this as empty value
     * <p>AS 2005 shows this as "(null)" value
     */
    public transient final StringProperty NullMemberRepresentation =
        new StringProperty(
            this, "mondrian.olap.NullMemberRepresentation", "#null");

    /**
     * Property that defines
     * the iteration limit when computing an aggregate; 0 indicates unlimited.
     */
    public transient final IntegerProperty IterationLimit =
        new IntegerProperty(
            this, "mondrian.rolap.iterationLimit", 0);

    /**
     * Property that defines
     * whether the <code>MemoryMonitor</code> should be enabled. By
     * default for Java5 and above it is not enabled.
     */
    public transient final BooleanProperty MemoryMonitor =
        new BooleanProperty(
            this, "mondrian.util.memoryMonitor.enable", false);

    /**
     * Property that defines
     * the default <code>MemoryMonitor</code> percentage threshold.
     */
    public transient final IntegerProperty MemoryMonitorThreshold =
        new IntegerProperty(
            this, "mondrian.util.memoryMonitor.percentage.threshold", 90);

    /**
     * Property that defines
     * the name of the class used as a memory monitor.
     *
     * <p>If the value is
     * non-null, it is used by the <code>MemoryMonitorFactory</code>
     * to create the implementation.
     */
    public transient final StringProperty MemoryMonitorClass =
        new StringProperty(
            this, "mondrian.util.MemoryMonitor.class", null);

    /**
     * Property that defines
     * the name of the class used to compile scalar expressions.
     *
     * <p>If the value is
     * non-null, it is used by the <code>ExpCompiler.Factory</code>
     * to create the implementation.
     */
    public transient final StringProperty ExpCompilerClass = new StringProperty(
        this, "mondrian.calc.ExpCompiler.class", null);

    /**
     * Property that defines
     * when to apply the crossjoin optimization algorithm.
     *
     * <p>If a crossjoin input list's size is larger than this property's
     * value and the axis has the "NON EMPTY" qualifier, then
     * the crossjoin non-empty optimizer is applied.
     * Setting this value to '0' means that for all crossjoin
     * input lists in non-empty axes will have the optimizer applied.
     * On the other hand, if the value is set larger than any possible
     * list, say <code>Integer.MAX_VALUE</code>, then the optimizer
     * will never be applied.
     */
    public transient final IntegerProperty CrossJoinOptimizerSize =
        new IntegerProperty(
            this, "mondrian.olap.fun.crossjoin.optimizer.size", 0);

    /**
     * Property that defines
     * the behavior of division if the denominator evaluates to zero.
     *
     * <p>If a division has a non-null numerator and a null denominator,
     * it evaluates to "Infinity", which conforms to MSAS behavior. However,
     * the old semantics of evaluating this to NULL (non MSAS-conforming), is
     * useful in some applications. This property controls whether the
     * result should be NULL if the denominator is Null.
     */
    public transient final BooleanProperty NullDenominatorProducesNull =
        new BooleanProperty(
            this, "mondrian.olap.NullDenominatorProducesNull", false);

    /**
     * Property that defines
     * whether to generate SQL queries using the <code>GROUPING SETS</code>
     * construct for rollup. By default it is not enabled.
     *
     * <p>Ignored on databases which do not support the
     * <code>GROUPING SETS</code> construct (see
     * {@link mondrian.spi.Dialect#supportsGroupingSets}).
     */
    public transient final BooleanProperty EnableGroupingSets =
        new BooleanProperty(
            this, "mondrian.rolap.groupingsets.enable", false);

    /**
     * Property that defines whether to ignore measure when non joining
     * dimension is in the tuple during aggregation.
     *
     * <p>If there are unrelated dimensions to a measure in context during
     * aggregation, the measure is ignored in the evaluation context. This
     * behaviour kicks in only if the cubeusage for this measure has
     * IgnoreUnrelatedDimensions attribute set to false.
     *
     * <p>For example, Gender doesn't join with [Warehouse Sales] measure.
     *
     * <p>With mondrian.olap.agg.IgnoreMeasureForNonJoiningDimension=true
     * Warehouse Sales gets eliminated and is ignored in the aggregate value.
     * <blockquote>
     * <p>                                    [Store Sales] + [Warehouse Sales]
     * SUM({Product.members * Gender.members})    7,913,333.82
     * </blockquote>
     * <p>With mondrian.olap.agg.IgnoreMeasureForNonJoiningDimension=false
     * Warehouse Sales with Gender All level member contributes to the aggregate
     * value.
     * <blockquote>
     * <p>                                     [Store Sales] + [Warehouse Sales]
     * SUM({Product.members * Gender.members})    9,290,730.03
     * </blockquote>
     * <p>On a report where Gender M, F and All members exist a user will see a
     * large aggregated value compared to the aggregated value that can be
     * arrived at by suming up values against Gender M and F. This can be
     * confusing to the user. This feature can be used to eliminate such a
     * situation.
     */
    public transient final BooleanProperty IgnoreMeasureForNonJoiningDimension =
        new BooleanProperty(
            this,
            "mondrian.olap.agg.IgnoreMeasureForNonJoiningDimension",
            false);

    /**
     * Property determines if elements of dimension (levels, hierarchies,
     * members) need to be prefixed with dimension name in MDX query.
     *
     * <p>For example when the property is true, the following queries
     * will error out. The same queries will work when this property
     * is set to false.
     * <blockquote>
     * <p>
     * select {[M]} on 0 from sales
     * <p>
     * select {[USA]} on 0 from sales
     * <p>
     * select {[USA].[CA].[Santa Monica]}  on 0 from sales
     * </blockquote>
     * <p>
     * When the property is set to true, any query where elements are
     * prefixed with dimension name as below will work
     * <blockquote>
     * <p>
     * select {[Gender].[F]} on 0 from sales
     * <p>
     * select {[Customers].[Santa Monica]} on 0 from sales
     * </blockquote>
     * <p>
     * Please note that this property does not govern the behaviour where in
     * <blockquote>
     * <p>
     * [Gender].[M]
     * </blockquote>
     * <p>
     * is resolved into a fully qualified
     * <blockquote>
     * <p>
     * [Gender].[All Gender].[M]
     * </blockquote>
     *
     * <p> In a scenario where the schema is very large and dimensions have
     * large number of members a MDX query that has a invalid member in it will
     * cause mondrian to to go through all the dimensions, levels, hierarchies,
     * members and properties trying to resolve the element name. This behavior
     * consumes considerable time and resources on the server. Setting this
     * property to true will make it fail fast in a scenario where it is
     * desirable.
     */
    public transient final BooleanProperty NeedDimensionPrefix =
        new BooleanProperty(
            this, "mondrian.olap.elements.NeedDimensionPrefix", false);

    /**
     * Property that determines whether to cache RolapCubeMember objects,
     * each of which associates a member of a shared hierarchy with a
     * particular cube in which it is being used.
     *
     * <p>The default is {@code true}, that is, use a cache. If you wish to use
     * the member cache control aspects of {@link mondrian.olap.CacheControl},
     * you must set this property to {@code false}.</p>
     *
     * <p>In future, we plan to make RolapCubeMember more lightweight to
     * construct, and we will probably obsolete this cache and this
     * property.</p>
     */
    public transient final BooleanProperty EnableRolapCubeMemberCache =
        new BooleanProperty(
            this, "mondrian.rolap.EnableRolapCubeMemberCache", true);

    /**
     * Property that controls the behavior of
     * {@link Property#SOLVE_ORDER solve order} of calculated members and sets.
     *
     * <p>Valid values are "absolute" and "scoped" (the default). See
     * {@link SolveOrderModeEnum} for details.</p>
     */
    public transient final StringProperty SolveOrderMode =
        new StringProperty(
            this, "mondrian.rolap.SolveOrderMode", SolveOrderModeEnum.ABSOLUTE.name());

    /**
     * Strategies for applying solve order, exposed via the property
     * {@link MondrianProperties#SolveOrderMode}.
     */
    public enum SolveOrderModeEnum {

        /**
         * The SOLVE_ORDER value is absolute regardless of
         * where it is defined; e.g. a query defined calculated
         * member with a SOLVE_ORDER of 1 always takes precedence
         * over a cube defined value of 2.
         *
         * <p>Compatible with Analysis Services 2000, and default behavior
         * up to mondrian-3.0.3.
         */
        ABSOLUTE,

        /**
         * Cube calculated members are resolved before any session
         * scope calculated members, and session scope members are
         * resolved before any query defined calculation.  The
         * SOLVE_ORDER value only applies within the scope in which
         * it was defined.
         *
         * <p>Compatible with Analysis Services 2005, and default behavior
         * from mondrian-3.0.4 and later.
         */
        SCOPED
    }

    /**
     * Property that defines
     * whether to enable new naming behavior.
     *
     * <p>If true, hierarchies are named [Dimension].[Hierarchy]; if false,
     * [Dimension.Hierarchy].
     */
    public transient final BooleanProperty SsasCompatibleNaming =
        new BooleanProperty(
            this, "mondrian.olap.SsasCompatibleNaming", false);
}

// End MondrianProperties.java
