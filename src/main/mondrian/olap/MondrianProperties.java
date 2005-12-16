/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 22 December, 2002
*/
package mondrian.olap;

import org.apache.log4j.Logger;
import org.eigenbase.util.property.*;

import javax.servlet.ServletContext;
import java.io.*;
import java.net.*;
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
 * @author jhyde
 * @since 22 December, 2002
 * @version $Id$
 */
public class MondrianProperties extends TriggerableProperties {

    private final FilePropertySource mondrianDotPropertiesSource =
            new FilePropertySource(new File(mondrianDotProperties));
    private final FilePropertySource buildDotPropertiesSource =
            new FilePropertySource(new File(buildDotProperties));
    private int populateCount;

    private static final Logger LOGGER =
            Logger.getLogger(MondrianProperties.class);

    /**
     * Properties, drawn from {@link System#getProperties}, plus the contents
     * of "mondrian.properties" if it exists. A singleton.
     */
    private static MondrianProperties instance;
    private static final String mondrianDotProperties = "mondrian.properties";
    private static final String buildDotProperties = "build.properties";
    private static final String servletPath =
            "/WEB-INF/" + mondrianDotProperties;


    /**
     * Returns the singleton.
     */
    public static synchronized MondrianProperties instance() {
        if (instance == null) {
            instance = new MondrianProperties();
            instance.populate(null);
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
         * Also updates
         */
        InputStream openStream();

        /**
         * Returns true if the source exists and has been modified since last
         * time we called {@link #openStream()}.
         */
        boolean isStale();

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
     * Loads this property set from: the file "mondrian.properties" (if it
     * exists); the "mondrian.properties" in the JAR (if we're in a servlet);
     * and from the system properties.
     *
     * @param servletContext May be null
     */
    public void populate(ServletContext servletContext) {
        // Read properties file "mondrian.properties", if it exists. If we have
        // read the file before, only read it if it is newer.
        loadIfStale(mondrianDotPropertiesSource);

        // For file-based installations (i.e. testing), load any overrides from
        // "build.properties".
        loadIfStale(buildDotPropertiesSource);

        // If we're in a servlet, read "mondrian.properties" from WEB-INF
        // directory.
        if (servletContext != null) {
            try {
                final URL resourceUrl = servletContext.getResource(servletPath);
                if (resourceUrl != null) {
                    final UrlPropertySource source =
                            new UrlPropertySource(resourceUrl);
                    loadIfStale(source);
                }
            } catch (MalformedURLException e) {
                LOGGER.error("Mondrian: could not load servlet resource '" +
                        servletPath + "'", e);
            }
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
     */
    private void load(final PropertySource source) {
        try {
            load(source.openStream());
            if (populateCount == 0) {
                LOGGER.info("Mondrian: properties loaded from '"
                    + source
                    + "'");
            }
        } catch (IOException e) {
            LOGGER.error("Mondrian: error while loading properties "
                + "from '"
                + source
                + "' ("
                + e
                + ")");
        }
    }

    /**
     * Maximum number of simultaneous queries the system will allow.
     */
    public final IntegerProperty QueryLimit = new IntegerProperty(
            this, "mondrian.query.limit", 40);

    /**
     * Property which controls the amount of tracing displayed.
     */
    public final IntegerProperty TraceLevel = new IntegerProperty(
            this, "mondrian.trace.level");

    /**
     * Property containing the name of the file to which tracing is to be
     * written.
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
     * Property which, if set to a value greatert than zero, limits the maximum
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
    public final BooleanProperty getFlushAfterQuery = new BooleanProperty(
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
     * If a dimension has more than this number of members, use a
     * smart member reader (see {@link mondrian.rolap.SmartMemberReader}).
     * Default is 100.
     */
    public final IntegerProperty LargeDimensionThreshold = new IntegerProperty(
            this, "mondrian.rolap.LargeDimensionThreshold", 100);

    /**
     * Property which, with {@link #SparseSegmentCountThreshold}, determines
     * whether to choose a sparse or dense representation when storing
     * collections of cell values.
     *
     * <p>When storing collections of cell values, we have to choose between a
     * sparse and a dense representation, based upon the <code>possible</code>
     * and <code>actual</code> number of values.
     * The <code>density</code> is <code>actual / possible</code>.
     * We use a sparse representation if
     *   <code>possible -
     *   {@link #SparseSegmentCountThreshold countThreshold} *
     *   actual &gt;
     *   {@link #SparseSegmentDensityThreshold densityThreshold}</code>
     *
     * <p>The default values are
     * {@link #SparseSegmentCountThreshold countThreshold} = 1000,
     * {@link #SparseSegmentDensityThreshold} = 0.5..
     *
     * <p>At these default values, we use a dense representation
     * for (1000 possible, 0 actual), or (2000 possible, 500 actual), or
     * (3000 possible, 1000 actual). Any fewer actual values, or any more
     * possible values, and we will use a sparse representation.
     */
    public final IntegerProperty SparseSegmentCountThreshold =
            new IntegerProperty(
                    this, "mondrian.rolap.SparseSegmentValueThreshold", 1000);

    public final DoubleProperty SparseSegmentDensityThreshold =
            new DoubleProperty(
                    this, "mondrian.rolap.SparseSegmentDensityThreshold", 0.5);

    /**
     * todo:
     */
    public final StringProperty QueryFilePattern = new StringProperty(
            this, "mondrian.test.QueryFilePattern", null);

    /**
     * todo:
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
     * Boolean property that controls whether aggregates should be used.
     *
     * <p>If true, then aggregates are used. This property is
     * queried prior to each aggregate query so that changing the value of this
     * property dynamically (not just at startup) is meaningful.
     * Aggregates can be read from the database using the
     * {@link #ReadAggregates} property but will not be used unless this
     * property is set to true.
     */
    public final BooleanProperty UseAggregates = new BooleanProperty(
            this, "mondrian.rolap.aggregates.Use", false);

    /**
     * Boolean property which determines whether aggregates should be read.
     *
     * <p>If set to true, then the database is scanned for aggregate tables.
     * Unless mondrian.rolap.aggregates.Use is set to true, the aggregates
     * found will not be used.
     */
    public final BooleanProperty ReadAggregates = new BooleanProperty(
            this, "mondrian.rolap.aggregates.Read", false);


    /**
     * Boolean property that controls whether aggregate tables
     * are ordered by their volume or row count.
     */
    public final BooleanProperty ChooseAggregateByVolume = new BooleanProperty(
            this, "mondrian.rolap.aggregates.ChooseByVolume", false);

    /**
     * String property which can be either a resource in the
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
     * Boolean property that controls whether or not triggers are
     * executed when a {@link MondrianProperties} property changes.
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
     * Boolean property which controls whether the MDX parser resolves uses
     * case-sensitive matching when looking up identifiers. The default is
     * false.
     */
    public final BooleanProperty CaseSensitive = new BooleanProperty(
            this, "mondrian.olap.case.sensitive", false);

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
     * <p>If this property is positive, Mondrian allocates an expression
     * evaluator which evaluates each expression several times, and makes sure
     * that the results of the expression are independent of dimensions which
     * the expression claims to be independent of.
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
     * Default value is null.
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
}

// End MondrianProperties.java
