/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 22 December, 2002
*/
package mondrian.olap;

import org.apache.log4j.Logger;
import org.eigenbase.util.property.*;
import org.eigenbase.util.property.Property;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

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
 * @since 22 December, 2002
 * @version $Id$
 */
public class MondrianProperties extends TriggerableProperties {

    private final FilePropertySource mondrianDotPropertiesSource =
            new FilePropertySource(new File(mondrianDotProperties));
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
        loadIfStale(mondrianDotPropertiesSource);

        URL url = null;
        File file = new File(mondrianDotProperties);
        if (file.exists() && file.isFile()) {
            // Read properties file "mondrian.properties" from PWD, if it
            // exists.
            try {
                url = file.toURI().toURL();
            } catch (MalformedURLException e) {
                LOGGER.warn("Mondrian: file '"
                    + file.getAbsolutePath()
                    + "' could not be loaded" , e);
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
            LOGGER.warn("mondrian.properties can't be found under '"
                    + new File(".").getAbsolutePath() + "' or classloader");
        }

        // copy in all system properties which start with "mondrian."
        int count = 0;
        for (Enumeration keys = System.getProperties().keys();
             keys.hasMoreElements(); ) {
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
                LOGGER.info("Mondrian: properties loaded from '"
                    + source.getDescription()
                    + "'");
            }
        } catch (IOException e) {
            LOGGER.error("Mondrian: error while loading properties "
                + "from '"
                + source.getDescription()
                + "' ("
                + e
                + ")");
        }
    }

    /**
     * Returns a list of every {@link org.eigenbase.util.property.Property}.
     *
     * <p>todo: Move to base class, {@link TriggerableProperties}, and rename
     * base method {@link TriggerableProperties#getProperties()}}.
     *
     * @return List of properties
     */
    public List<Property> getPropertyList() {
        Field[] fields = getClass().getFields();
        List<Property> list = new ArrayList<Property>();
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers()) &&
                Property.class.isAssignableFrom(
                    field.getType())) {
                try {
                    list.add((Property) field.get(this));
                } catch (IllegalAccessException e) {
                    throw Util.newInternal(
                        e,
                        "While accessing property '" + field.getName() + "'");
                }
            }
        }
        return list;
    }

    /**
     * Returns the definition of a named property, or null if there is no
     * such property.
     *
     * <p>todo: Move to base class, {@link TriggerableProperties}.
     *
     * @param path Name of the property
     * @return Definition of property, or null if there is no property with this
     *   name
     */
    public Property getPropertyDefinition(String path) {
        final List<Property> propertyList = getPropertyList();
        for (Property property : propertyList) {
            if (property.getPath().equals(path)) {
                return property;
            }
        }
        return null;
    }

    /**
     * Maximum number of simultaneous queries the system will allow.
     *
     * <p>Oracle fails if you try to run more than the 'processes' parameter in
     * init.ora, typically 150. The throughput of Oracle and other databases
     * will probably reduce long before you get to their limit.</p>
     */
    public final IntegerProperty QueryLimit = new IntegerProperty(
            this, "mondrian.query.limit", 40);

    /**
     * Property which controls the amount of tracing displayed.
     *
     * <p>If trace level is above 0, SQL tracing will be enabled and logged as
     * per the <code>{@link #DebugOutFile mondrian.debug.out.file}</code>
     * property below. This is separate from Log4j logging.
     */
    public final IntegerProperty TraceLevel = new IntegerProperty(
            this, "mondrian.trace.level");

    /**
     * Property containing the name of the file to which tracing is to be
     * written. If empty (the default), prints to stdout.
     */
    public final StringProperty DebugOutFile = new StringProperty(
            this, "mondrian.debug.out.file", null);

    /**
     * Property containing a list of JDBC drivers to load automatically.
     * Must be a comma-separated list of class names, and the classes must be
     * on the class path.
     */
    public final StringProperty JdbcDrivers = new StringProperty(
            this,
            "mondrian.jdbcDrivers",
            "sun.jdbc.odbc.JdbcOdbcDriver," +
            "org.hsqldb.jdbcDriver," +
            "oracle.jdbc.OracleDriver," +
            "com.mysql.jdbc.Driver");

    /**
     * Property which, if set to a value greater than zero, limits the maximum
     * size of a result set.
     */
    public final IntegerProperty ResultLimit = new IntegerProperty(
            this, "mondrian.result.limit", 0);

    /** @deprecated obsolete */
    public final IntegerProperty CachePoolCostLimit = new IntegerProperty(
            this, "mondrian.rolap.CachePool.costLimit", 10000);

    /** @deprecated obsolete */
    public final BooleanProperty PrintCacheablesAfterQuery =
            new BooleanProperty(
                    this, "mondrian.rolap.RolapResult.printCacheables");

    /** @deprecated obsolete */
    public final BooleanProperty FlushAfterQuery = new BooleanProperty(
            this, "mondrian.rolap.RolapResult.flushAfterEachQuery");

    // mondrian.test properties

    /**
     * Property which determines which tests are run.
     * This is a regular expression as defined by
     * {@link java.util.regex.Pattern}.
     * If this property is specified, only tests whose names match the pattern
     * in its entirety will be run.
     *
     * @see #TestClass
     */
    public final StringProperty TestName = new StringProperty(
            this, "mondrian.test.Name", null);

    /**
     * Property which determines which test class to run.
     * This is the name of the class which either implements
     * <code>junit.framework.Test</code> or has a method
     * <code>public [static] junit.framework.Test suite()</code>.
     *
     * <p>Example:
     * <blockquote><code>mondrian.test.Class=mondrian.test.FoodMartTestCase</code></blockquote>
     * </p>
     *
     * @see #TestName
     */
    public final StringProperty TestClass = new StringProperty(
            this, "mondrian.test.Class", null);

    /**
     * Property containing the connect string which regresssion tests should
     * use to connect to the database.
     * Format is specified in {@link Util#parseConnectString(String)}.
     */
    public final StringProperty TestConnectString = new StringProperty(
            this, "mondrian.test.connectString", null);


    // miscellaneous

    /**
     * Property containing the JDBC URL of the FoodMart database.
     * The default value is to connect to an ODBC data source called
     * "MondrianFoodMart".
     */
    public final StringProperty FoodmartJdbcURL = new StringProperty(
            this, "mondrian.foodmart.jdbcURL", "jdbc:odbc:MondrianFoodMart");

    /**
     * Property containing the JDBC URL of a test database.
     * It does not default.
     */
    public final StringProperty TestJdbcURL = new StringProperty(
            this, "mondrian.test.jdbcURL", null);

    /**
     * Property containing the JDBC user of a test database.
     * The default value is null, to cope with DBMSs that don't need this.
     */
    public final StringProperty TestJdbcUser = new StringProperty(
            this, "mondrian.test.jdbcUser", null);

    /**
     * Property containing the JDBC password of a test database.
     * The default value is null, to cope with DBMSs that don't need this.
     */
    public final StringProperty TestJdbcPassword = new StringProperty(
            this, "mondrian.test.jdbcPassword", null);

    /**
     * Property which determines when a dimension is considered "large".
     * If a dimension has more than this number of members, Mondrian uses a
     * {@link mondrian.rolap.SmartMemberReader smart member reader}.
     */
    public final IntegerProperty LargeDimensionThreshold = new IntegerProperty(
            this, "mondrian.rolap.LargeDimensionThreshold", 100);

    /**
     * Property which, with {@link #SparseSegmentDensityThreshold}, determines
     * whether to choose a sparse or dense representation when storing
     * collections of cell values in memory.
     *
     * <p>When storing collections of cell values, Mondrian has to choose
     * between a sparse and a dense representation, based upon the
     * <code>possible</code> and <code>actual</code> number of values.
     * The <code>density</code> is <code>actual / possible</code>.
     *
     * <p>We use a sparse representation if
     *   <code>(possible -
     *   {@link #SparseSegmentCountThreshold countThreshold}) *
     *   {@link #SparseSegmentDensityThreshold densityThreshold} &gt;
     *   actual</code>
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
    public final IntegerProperty SparseSegmentCountThreshold =
            new IntegerProperty(
                    this, "mondrian.rolap.SparseSegmentValueThreshold", 1000);

    /**
     * Property which, with {@link #SparseSegmentCountThreshold},
     * determines whether to choose a sparse or dense representation when
     * storing collections of cell values in memory.
     */
    public final DoubleProperty SparseSegmentDensityThreshold =
            new DoubleProperty(
                    this, "mondrian.rolap.SparseSegmentDensityThreshold", 0.5);

    /**
     * Property which defines
     * a pattern for which test XML files to run.  Pattern has to
     * match a file name of the form:
     * <code>query<i>whatever</i>.xml</code> in the directory.
     *
     * <p>Example:
     * <blockquote><code>mondrian.test.QueryFilePattern=queryTest_fec[A-Za-z0-9_]*.xml</code></blockquote>
     * </p>
     */
    public final StringProperty QueryFilePattern = new StringProperty(
            this, "mondrian.test.QueryFilePattern", null);

    /**
     * Property defining
     * where the test XML files are.
     */
    public final StringProperty QueryFileDirectory = new StringProperty(
            this, "mondrian.test.QueryFileDirectory", null);

    /**
     * todo:
     */
    public final IntegerProperty Iterations = new IntegerProperty(
            this, "mondrian.test.Iterations", 1);

    /**
     * todo:
     */
    public final IntegerProperty VUsers = new IntegerProperty(
            this, "mondrian.test.VUsers", 1);

    /**
     * Property which returns the time limit for the test run in seconds.
     * If the test is running after that time, it is terminated.
     */
    public final IntegerProperty TimeLimit = new IntegerProperty(
            this, "mondrian.test.TimeLimit", 0);

    /**
     * Property which indicates whether this is a "warmup test".
     */
    public final BooleanProperty Warmup = new BooleanProperty(
            this, "mondrian.test.Warmup", false);

    /**
     * Property which contains the URL of the catalog to be used by
     * {@link mondrian.tui.CmdRunner} and XML/A Test.
     */
    public final StringProperty CatalogURL = new StringProperty(
            this, "mondrian.catalogURL", null);

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
    public final BooleanProperty UseAggregates = new BooleanProperty(
            this, "mondrian.rolap.aggregates.Use", false);

    /**
     * Boolean property which determines whether Mondrian should read aggregate
     * tables.
     *
     * <p>If set to true, then Mondrian scans the database for aggregate tables.
     * Unless mondrian.rolap.aggregates.Use is set to true, the aggregates
     * found will not be used.
     */
    public final BooleanProperty ReadAggregates = new BooleanProperty(
            this, "mondrian.rolap.aggregates.Read", false);


    /**
     * Boolean property that controls whether aggregate tables
     * are ordered by their volume or row count.
     *
     * <p>If true, Mondrian uses the aggregate table with the smallest volume
     * (number of rows multiplied by number of columns); if false, Mondrian
     * uses the aggregate table with the fewest rows.
     */
    public final BooleanProperty ChooseAggregateByVolume = new BooleanProperty(
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
    public final StringProperty AggregateRules = new StringProperty(
            this, "mondrian.rolap.aggregates.rules", "/DefaultRules.xml");

    /**
     * String property which is the AggRule element's tag value.
     *
     * <p>Normally, this property is not set by a user.
     */
    public final StringProperty AggregateRuleTag = new StringProperty(
            this, "mondrian.rolap.aggregates.rule.tag", "default");

    /**
     * Boolean property which controls whether to print the SQL code
     * generated for aggregate tables.
     *
     * <p>If set, then as each aggregate request is processed, both the lost
     * and collapsed dimension create and insert sql code is printed.
     * This is for use in the CmdRunner allowing one to create aggregate table
     * generation sql.
     */
    public final BooleanProperty GenerateAggregateSql = new BooleanProperty(
            this, "mondrian.rolap.aggregates.generateSql", false);

    //
    //////////////////////////////////////////////////////////////////////////

    /**
     * Boolean property that controls whether a RolapStar's
     * aggregate data cache is cleared after each query.
     * If true, no RolapStar will cache aggregate data from one
     * query to the next (the cache is cleared after each query).
     */
    public final BooleanProperty DisableCaching = new BooleanProperty(
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
    public final BooleanProperty EnableTriggers = new BooleanProperty(
            this, "mondrian.olap.triggers.enable", true);

    /**
     * Boolean property which controls pretty-print mode.
     * If set to true, the all SqlQuery SQL strings
     * will be generated in pretty-print mode, formatted for ease of reading.
     */
    public final BooleanProperty GenerateFormattedSql = new BooleanProperty(
            this, "mondrian.rolap.generate.formatted.sql", false);

    /**
     * Boolean property which controls whether each query axis implicit has the
     * NON EMPTY option set. The default is false.
     */
    public final BooleanProperty EnableNonEmptyOnAllAxis = new BooleanProperty(
            this, "mondrian.rolap.nonempty", false);

    /**
     * Boolean property which controls whether sibling members are
     * compared according to order key value fetched from their ordinal
     * expression.  The default is false (only database ORDER BY is used).
     */
    public final BooleanProperty CompareSiblingsByOrderKey =
        new BooleanProperty(
            this, "mondrian.rolap.compareSiblingsByOrderKey",
            false);

    /**
     * Boolean property which controls whether to use a cache for frequently
     * evaluated expressions. With the cache disabled, an expression like
     * <code>Rank([Product].CurrentMember,
     * Order([Product].MEMBERS, [Measures].[Unit Sales]))</code> would perform
     * many redundant sorts. The default is true.
     */
    public final BooleanProperty EnableExpCache = new BooleanProperty(
            this, "mondrian.expCache.enable", true);


    /**
     * Integer property which controls whether to test operators' dependencies,
     * and how much time to spend doing it.
     *
     * <p>If this property is positive, Mondrian's test framework allocates an
     * expression evaluator which evaluates each expression several times, and
     * makes sure that the results of the expression are independent of
     * dimensions which the expression claims to be independent of.
     *
     * <p>The default is 0.
     */
    public final IntegerProperty TestExpDependencies = new IntegerProperty(
            this, "mondrian.test.ExpDependencies", 0);

    /**
     * Seed for random number generator used by some of the tests.
     *
     * <p>
     * Any value besides 0 or -1 gives deterministic behavior.
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
    public final IntegerProperty TestSeed = new IntegerProperty(
            this, "mondrian.test.random.seed", 1234);

    /**
     * Name of locale property file.
     *
     * <p>Used for the {@link mondrian.rolap.DynamicSchemaProcessor
     * LocalizingDynamicSchemaProcessor}; see
     * <a href="{@docRoot}/../schema.html#I18n">Internationalization</a>
     * for more details.</td>
     *
     * <p>Default value is null.
     */
    public final StringProperty LocalePropFile = new StringProperty(
            this, "mondrian.rolap.localePropFile", null);

    /**
     * if enabled some NON EMPTY CrossJoin will be computed in SQL
     */
    public final BooleanProperty EnableNativeCrossJoin = new BooleanProperty(
        this, "mondrian.native.crossjoin.enable", true);

    /**
     * if enabled some TopCount will be computed in SQL
     */
    public final BooleanProperty EnableNativeTopCount = new BooleanProperty(
        this, "mondrian.native.topcount.enable", true);

    /**
     * if enabled some Filter() will be computed in SQL
     */
    public final BooleanProperty EnableNativeFilter = new BooleanProperty(
        this, "mondrian.native.filter.enable", true);

    /**
     * some NON EMPTY set operations like member.children, level.members and
     * member descendants will be computed in SQL
     */
    public final BooleanProperty EnableNativeNonEmpty = new BooleanProperty(
        this, "mondrian.native.nonempty.enable", true);

    /**
     * Alerting action to take in case native evaluation of a function is
     * enabled but not supported for that function's usage in a particular
     * query.  (No alert is ever raised in cases where native evaluation would
     * definitely have been wasted effort.)
     *
     *<p>
     *
     * Recognized actions:
     *
     *<ul>
     *
     *<li><code>OFF</code>:  do nothing (default action, also used if
     * unrecognized action is specified)
     *
     *<li><code>WARN</code>:  log a warning to RolapUtil logger
     *
     *<li><code>ERROR</code>:  throw an instance of
     * {@link NativeEvaluationUnsupportedException}
     *
     *</ul>
     */
    public final StringProperty AlertNativeEvaluationUnsupported =
        new StringProperty(this, "mondrian.native.unsupported.alert", "OFF");

    /**
     * If enabled, first row in the result of an XML/A drill-through request
     * will be filled with the total count of rows in underlying database.
     */
    public final BooleanProperty EnableTotalCount = new BooleanProperty(
        this, "mondrian.xmla.drillthroughTotalCount.enable", true);

    /**
     * Boolean property which controls whether the MDX parser resolves uses
     * case-sensitive matching when looking up identifiers. The default is
     * false.
     */
    public final BooleanProperty CaseSensitive = new BooleanProperty(
            this, "mondrian.olap.case.sensitive", false);


    /**
     * Property which defines
     * limit on the number of rows returned by XML/A drill through request.
     */
    public final IntegerProperty MaxRows = new IntegerProperty(
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
    public final IntegerProperty MaxConstraints = new IntegerProperty(
        this, "mondrian.rolap.maxConstraints", 1000);

    /**
     * Property which defines the
     * maximum number of passes allowable while evaluating an MDX expression.
     * If evaluation exceeds this depth (for example, while evaluating a
     * very complex calculated member), Mondrian will throw an error.
     */
    public final IntegerProperty MaxEvalDepth = new IntegerProperty(
            this, "mondrian.rolap.evaluate.MaxEvalDepth", 10);


    public final StringProperty JdbcFactoryClass = new StringProperty(
            this, "mondrian.rolap.aggregates.jdbcFactoryClass", null);

    /**
     * Timeout value (in seconds) for queries; 0 indicates no timeout
     */
    public final IntegerProperty QueryTimeout = new IntegerProperty(
        this, "mondrian.rolap.queryTimeout", 0);


    /**
     * Whether non-existent member errors should be ignored during schema
     * load
     */
    public final BooleanProperty IgnoreInvalidMembers = new BooleanProperty(
        this, "mondrian.rolap.ignoreInvalidMembers", false);

    /**
     * Iteration limit when computing an aggregate; 0 indicates unlimited
     */
    public final IntegerProperty IterationLimit = new IntegerProperty(
        this, "mondrian.rolap.iterationLimit", 0);

    /**
     * Whether the <code>MemoryMonitor</code> should be enabled. By
     * default for Java5 and above it is not enabled.
     */
    public final BooleanProperty MemoryMonitor = new BooleanProperty(
        this, "mondrian.util.memoryMonitor.enable", false);

    /**
     * The default <code>MemoryMonitor</code> percentage threshold.
     */
    public final IntegerProperty MemoryMonitorThreshold = new IntegerProperty(
        this, "mondrian.util.memoryMonitor.percentage.threshold", 90);

    /**
     * The <code>MemoryMonitor</code> class property. If the value is
     * non-null, it is used by the <code>MemoryMonitorFactory</code>
     * to create the implementation.
     */
    public final StringProperty MemoryMonitorClass = new StringProperty(
            this, "mondrian.util.MemoryMonitor.class", null);

    /**
     * The <code>ExpCompiler</code> class property. If the value is
     * non-null, it is used by the <code>ExpCompiler.Factory</code>
     * to create the implementation.
     */
    public final StringProperty ExpCompilerClass = new StringProperty(
            this, "mondrian.calc.ExpCompiler.class", null);

    /**
     * If a crossjoin input list's size is larger than this property's
     * value and the axis has the "NON EMPTY" qualifier, then
     * the crossjoin non-empty optimizer is applied.
     * Setting this value to '0' means that for all crossjoin
     * input lists in non-empty axes will have the optimizer applied.
     * On the other hand, if the value is set larger than any possible
     * list, say <code>Integer.MAX_VALUE</code>, then the optimizer
     * will never be applied.
     */
    public final IntegerProperty CrossJoinOptimizerSize = new IntegerProperty(
            this, "mondrian.olap.fun.crossjoin.optimizer.size", 0);

}

// End MondrianProperties.java
