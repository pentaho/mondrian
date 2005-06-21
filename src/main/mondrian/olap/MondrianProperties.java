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

import mondrian.util.PropertiesPlus;

import org.apache.log4j.Logger;
import javax.servlet.ServletContext;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Method;
import java.lang.ref.WeakReference;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.Iterator;
import java.util.List;

/**
 * <code>MondrianProperties</code> contains the properties which determine the
 * behavior of a mondrian instance.
 *
 * <p>There is a method for property valid in a
 * <code>mondrian.properties</code> file. Although it is possible to retrieve
 * properties using the inherited {@link Properties#getProperty(String)}
 * method, we recommend that you use methods in this class.
 *
 * <p>If you wish to be notified of changes to properties, use
 * {@link #addTrigger(mondrian.olap.MondrianProperties.Trigger, String)}
 * method to register a callback.
 *
 * @author jhyde
 * @since 22 December, 2002
 * @version $Id$
 **/
public class MondrianProperties extends PropertiesPlus {
    private static final Logger LOGGER =
            Logger.getLogger(MondrianProperties.class);

    /**
     * Properties, drawn from {@link System#getProperties}, plus the contents
     * of "mondrian.properties" if it exists. A singleton.
     */
    private static MondrianProperties instance;
    private static final String mondrianDotProperties = "mondrian.properties";
    private static final String buildDotProperties = "build.properties";


    public static synchronized MondrianProperties instance() {
        if (instance == null) {
            instance = new MondrianProperties();
            instance.populate(null);
        }
        return instance;
    }

    /**
     * A Trigger is a callback which allows a subscriber to be notified
     * when a property value changes.
     *
     * <p>If the user wishes to be able to remove a Trigger at some time after
     * it has been added, then either 1) the user has to keep the instance of
     * the Trigger that was added and use it when calling the remove method or
     * 2) the Trigger must implement the equals method and the Trigger used
     * during the call to the remove method must be equal to the original
     * Trigger added.
     * <p>
     * Each non-persistent Trigger is wrapped in a {@link WeakReference}, so
     * that is can be garbage-collected. But this means that
     * the user had better keep a reference to the Trigger, otherwise
     * it will be removed (garbage collected) without the user knowing.
     * <p>
     * Persistent Triggers (those that refer to objects in their
     * {@link #executeTrigger} method that will never be garbage-collected) are
     * not wrapped in a WeakReference.
     * <p>
     * What does all this mean, well, objects that might be garbage collected
     * that create a Trigger must keep a reference to the Trigger (otherwise the
     * Trigger will be garbage-collected out from under the object). A common
     * usage pattern is to implement the Trigger interface using an anonymous
     * class - anonymous classes are non-static classes when created in an
     * instance object. But, remember, the anonymous class instance holds a
     * reference to the outer instance object but not the other way around; the
     * instance object does not have an implicit reference to the anonymous
     * class instance - the reference must be explicit. And, if the anonymous
     * Trigger is created with the isPersistent method returning true, then,
     * surprise, the outer instance object will never be garbage collected!!!
     * <p>
     * Note that it is up to the creator of MondrianProperties Triggers to make
     * sure that they are ordered correctly. This is done by either having
     * order independent Triggers (they all have the same phase) or by
     * assigning phases to the Triggers where primaries execute before
     * secondary which execute before tertiary.
     * <p>
     * If a finer level of execution order granularity is needed, then the
     * implementation should be changed so that the {@link #phase} method
     * returns just some integer and it's up to the users to coordinate their
     * values.
     */
    public interface Trigger {
        int PRIMARY_PHASE     = 1;
        int SECONDARY_PHASE   = 2;
        int TERTIARY_PHASE    = 3;

        public static class VetoRT extends RuntimeException {
            public VetoRT(String msg) {
                super(msg);
            }
            public VetoRT(Exception ex) {
                super(ex);
            }
        }

        /**
         * An Entry is associated with a property key. Each Entry can have one
         * or more Triggers. Each Trigger is stored in a WeakReference so that
         * when the the Trigger is only reachable via weak referencs the Trigger
         * will be be collected and the contents of the WeakReference
         * will be set to null.
         */
        public static class Entry {
            private ArrayList triggerList;

            Entry() {
                triggerList = new ArrayList();
            }

            /**
             * Add a Trigger wrapping it in a WeakReference.
             *
             * @param trigger
             */
            void add(final Trigger trigger) {
                // this is the object to add to list
                Object o = (trigger.isPersistent())
                            ? trigger : (Object) new WeakReference(trigger);

                // Add a Trigger in the correct group of phases in the list
                for (ListIterator it = triggerList.listIterator();
                                it.hasNext(); ) {
                    Trigger t = convert(it.next());

                    if (t == null) {
                        it.remove();
                    } else if (trigger.phase() < t.phase()) {
                        // add it before
                        it.hasPrevious();
                        it.add(o);
                        return;
                    } else if (trigger.phase() == t.phase()) {
                        // add it after
                        it.add(o);
                        return;
                    }
                }
                triggerList.add(o);
            }

            /**
             * Remove the given Trigger.
             * In addition, any WeakReference that is empty are removed.
             *
             * @param trigger
             */
            void remove(final Trigger trigger) {
                for (Iterator it = triggerList.iterator(); it.hasNext(); ) {
                    Trigger t = convert(it.next());

                    if (t == null) {
                        it.remove();
                    } else if (t.equals(trigger)) {
                        it.remove();
                    }
                }
            }

            /**
             * Returns true if there are no Trigger in this Entry.
             *
             * @return true it there are no Triggers.
             */
            boolean isEmpty() {
                return triggerList.isEmpty();
            }

            /**
             * Execute all Triggers in this Entry passing in the property
             * key whose change was the casue.
             * In addition, any WeakReference that is empty are removed.
             *
             * @param key
             */
            void execute(final String key,
                         final String value) throws VetoRT {
                // Make a copy so that if during the execution of a trigger a
                // Trigger is added or removed, we do not get a concurrent
                // modification exception. We do an explicit copy (rather than
                // a clone) so that we can remove any WeakReference whose
                // content has become null.
                List l = new ArrayList();
                for (Iterator it = triggerList.iterator(); it.hasNext(); ) {
                    Trigger t = convert(it.next());

                    if (t == null) {
                        it.remove();
                    } else {
                        l.add(t);
                    }
                }

                for (Iterator it = l.iterator(); it.hasNext(); ) {
                    Trigger t = (Trigger) it.next();
                    t.executeTrigger(key, value);
                }

            }
            private Trigger convert(Object o) {
                if (o instanceof WeakReference) {
                    o = ((WeakReference) o).get();
                }
                return (Trigger) o;
            }
        }

        /**
         * If a Trigger is associated with a class or singleton, then it should
         * return true because its associated object is not subject to garbage
         * collection. On the other hand, if a Trigger is associated with an
         * object which will be garbage collected, then this method must return
         * false so that the Trigger will be wrapped in a WeakReference and
         * thus can itself be garbage collected.
         *
         * @return
         */
        boolean isPersistent();

        /**
         * Which phase does this Trigger belong to.
         *
         * @return
         */
        int phase();

        /**
         * Execute the Trigger passing in the key of the property whose change
         * triggered the execution.
         *
         * @param key
         */
        void executeTrigger(final String key, final String value) throws VetoRT;
    }

    private int populateCount;
    private Map triggers;
    private String[] propertyNames;

    public MondrianProperties() {
        triggers = Collections.EMPTY_MAP;
    }

    /**
     * Add a {@link Trigger} associating it with the parameter property key.
     *
     * @param trigger
     * @param key
     */
    public void addTrigger(Trigger trigger, String key) {
        if (triggers == Collections.EMPTY_MAP) {
            triggers = new HashMap();
        }

        Trigger.Entry e = (Trigger.Entry) triggers.get(key);
        if (e == null) {
            e = new Trigger.Entry();
            triggers.put(key, e);
        }
        e.add(trigger);
    }

    /**
     * Removes the {@link Trigger}, if any, that is associated with property
     * key.
     *
     * @param trigger
     * @param key
     */
    public void removeTrigger(Trigger trigger, String key) {
        Trigger.Entry e = (Trigger.Entry) triggers.get(key);
        if (e != null) {
            e.remove(trigger);
            if (e.isEmpty()) {
                triggers.remove(key);
            }
        }
    }

    /**
     * Sets the value of a property.
     *
     * <p>If the previous value does not equal the new value, executes any
     * {@link Trigger}s associated with the property, in order of their
     * {@link mondrian.olap.MondrianProperties.Trigger#phase() phase}.
     *
     * @param key
     * @param value
     * @return the old value
     */
    public synchronized Object setProperty(
            final String key, final String value) {
        String oldValue = getPropertyValueReflect(key);

        Object object = super.setProperty(key, value);
        if (oldValue == null) {
            oldValue = (object == null) ? null : object.toString();
        }

        // If we have changed the value of a property, then call all of the
        // property's associated Triggers (if any).
        if (! Util.equals(oldValue, value) && getEnableTriggers()) {
            Trigger.Entry e = (Trigger.Entry) triggers.get(key);
            if (e != null) {
                try {
                    e.execute(key, value);
                } catch (Trigger.VetoRT vex) {
                    // Reset to the old value, do not call setProperty
                    // unless you want to run out of stack space
                    superSetProperty(key, oldValue);
                    try {
                        e.execute(key, oldValue);
                    } catch (Trigger.VetoRT ex) {
                        // ignore during reset
                    }

                    throw vex;
                }
            }
        }

        return oldValue;
    }

    /**
     * This is ONLY called during a veto
     * operation. It calls the super class {@link #setProperty}.
     *
     * @param key
     * @param oldValue
     */
    private void superSetProperty(String key, String oldValue) {
        if (oldValue != null) {
            super.setProperty(key, oldValue);
        }
    }

    /**
     * Gets property value for given propertyName parameter by using reflection
     * on the {@link MondrianProperties} class. Returns null if there are any
     * exceptions or the field name prepended with "get" does not match the
     * method name.
     *
     * @param propertyName
     * @return
     */
    public String getPropertyValueReflect(String propertyName) {
        try {
            String fieldName = null;

            Field[] fields = MondrianProperties.class.getFields();
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                if (field.getType() != String.class) {
                    continue;
                }
                String v = (String) field.get(MondrianProperties.class);
                if (Util.equals(v, propertyName)) {
                    fieldName = field.getName();
                    break;
                }
            }

            if (fieldName != null) {
                String methodName = "get" + fieldName;
                Method m = MondrianProperties.class.getMethod(methodName, null);

                if (m != null) {
                    Object o =
                        m.invoke(MondrianProperties.instance(), new Object[0]);
                    if (o != null) {
                        return o.toString();
                    }
                }
            }

        } catch (Exception ex) {
            // ignore
        }
        return null;
    }

    /** 
     * Get the names of all of the mondrian properties. This uses reflection
     * to get all static class variable of type <code>String</code> which
     * do not end with "Default" - these are the variables holding names
     * of the properties. This value is cached.
     * 
     * @return array of property names.
     */
    public String[] getPropertyNamesReflect() {
        if (propertyNames == null) {
            List list = new ArrayList();
            try {
                Field[] fields = MondrianProperties.class.getFields();
                for (int i = 0; i < fields.length; i++) {
                    Field field = fields[i];
                    if (! Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }
                    if (field.getType() != String.class) {
                        continue;
                    }
                    if (field.getName().endsWith("Default")) {
                        continue;
                    }
                    String v = (String) field.get(MondrianProperties.class);
                    list.add(v);
                }
            } catch (Exception ex) {
                // ignore
            }
            propertyNames = (String[]) list.toArray(new String[list.size()]);
        }
        return propertyNames;
    }
    /**
     * Loads this property set from: the file "mondrian.properties" (if it
     * exists); the "mondrian.properties" in the JAR (if we're in a servlet);
     * and from the system properties.
     *
     * @param servletContext May be null
     */
    public void populate(ServletContext servletContext) {
        // Read properties file "mondrian.properties", if it exists.
        File file = new File(mondrianDotProperties);
        if (file.exists()) {
            try {
                URL url = Util.toURL(file);
                load(url);

                // For file-based installations (i.e. testing), load any overrides from
                // build.properties.
                file = new File(buildDotProperties);

                if (file.exists()) {
                    url = Util.toURL(file);
                    load(url);
                }
            } catch (MalformedURLException e) {
                LOGGER.error("Mondrian: file '"
                    + file.getAbsolutePath()
                    + "' could not be loaded ("
                    + e
                    + ")");
            }
        } else if (populateCount == 0 && false) {
            LOGGER.warn("Mondrian: Warning: file '"
                + file.getAbsolutePath()
                + "' not found");
        }

        // If we're in a servlet, read "mondrian.properties" from WEB-INF
        // directory.
        if (servletContext != null) {
            try {
                final URL resourceUrl = servletContext.getResource(
                        "/WEB-INF/" + mondrianDotProperties);
                if (resourceUrl != null) {
                    load(resourceUrl);
                }
                else if (populateCount == 0 && false) {
                    LOGGER.warn("Mondrian: Warning: servlet resource '"
                        + mondrianDotProperties
                        + "' not found");
                }
            } catch (MalformedURLException e) {
                LOGGER.error("Mondrian: '" + mondrianDotProperties
                    + "' could not be loaded from servlet context ("
                    + e
                    + ")");
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
     * Tries to load properties from a URL. Does not fail, just prints success
     * or failure to log.
     */
    private void load(final URL url) {
        try {
            load(url.openStream());
            if (populateCount == 0) {
                LOGGER.info("Mondrian: properties loaded from '"
                    + url
                    + "'");
            }
        } catch (IOException e) {
            LOGGER.error("Mondrian: error while loading properties "
                + "from '"
                + url
                + "' ("
                + e
                + ")");
        }
    }

    /**
     * Retrieves the value of the {@link #QueryLimit} property,
     * default value {@link #QueryLimit_Default}.
     */
    public int getQueryLimit() {
        return getIntProperty(QueryLimit, QueryLimit_Default);
    }
    /** Property {@value}. */
    public static final String QueryLimit = "mondrian.query.limit";
    /** Value is {@value}. */
    public static final int QueryLimit_Default = 40;

    /** Retrieves the value of the {@link #TraceLevel} property. */
    public int getTraceLevel() {
        return getIntProperty(TraceLevel);
    }
    /** Property {@value}. */
    public static final String TraceLevel = "mondrian.trace.level";

    /** Property {@value}. */
    public static final String DebugOutFile = "mondrian.debug.out.file";

    /** Retrieves the value of the {@link #JdbcDrivers} property,
     * default value {@link #JdbcDrivers_Default}. */
    public String getJdbcDrivers() {
        return getProperty(JdbcDrivers, JdbcDrivers_Default);
    }
    /** Property {@value}. */
    public static final String JdbcDrivers = "mondrian.jdbcDrivers";
    /** Values is {@value}. */
    public static final String JdbcDrivers_Default =
            "sun.jdbc.odbc.JdbcOdbcDriver,"
            + "org.hsqldb.jdbcDriver,"
            + "oracle.jdbc.OracleDriver,"
            + "com.mysql.jdbc.Driver";

    /** Retrieves the value of the {@link #ResultLimit} property. */
    public int getResultLimit() {
        return getIntProperty(ResultLimit, 0);
    }
    /** Property {@value}. */
    public static final String ResultLimit = "mondrian.result.limit";

    // mondrian.rolap properties

    /** Retrieves the value of the {@link #CachePoolCostLimit} property,
     * default value {@link #CachePoolCostLimit_Default}. */
    public int getCachePoolCostLimit() {
        return getIntProperty(CachePoolCostLimit, CachePoolCostLimit_Default);
    }
    /** Property {@value}. */
    public static final String CachePoolCostLimit = "mondrian.rolap.CachePool.costLimit";
    /** Value is {@value}. */
    public static final int CachePoolCostLimit_Default = 10000;

    /** Retrieves the value of the {@link #PrintCacheablesAfterQuery} property. */
    public boolean getPrintCacheablesAfterQuery() {
        return getBooleanProperty(PrintCacheablesAfterQuery);
    }
    /** Property {@value}. */
    public static final String PrintCacheablesAfterQuery =
                "mondrian.rolap.RolapResult.printCacheables";

    /** Retrieves the value of the {@link #FlushAfterQuery} property. */
    public boolean getFlushAfterQuery() {
        return getBooleanProperty(FlushAfterQuery);
    }
    /** Property {@value}. */
    public static final String FlushAfterQuery =
                "mondrian.rolap.RolapResult.flushAfterEachQuery";

    // mondrian.test properties

    /**
     * Retrieves the value of the {@link #TestName} property.
     * This is a regular expression as defined by
     * {@link java.util.regex.Pattern}.
     * If this property is specified, only tests whose names match the pattern
     * in its entirety will be run.
     *
     * @see #getTestClass
     */
    public String getTestName() {
        return getProperty(TestName);
    }
    /** Property {@value}. */
    public static final String TestName = "mondrian.test.Name";

    /**
     * Retrieves the value of the {@link #TestClass} property. This is the
     * name of the class which either implements {@link junit.framework.Test},
     * or has a method <code>public [static] {@link junit.framework.Test}
     * suite()</code>.
     * @see #getTestName
     */
    public String getTestClass() {
        return getProperty(TestClass);
    }
    /** Property {@value}. */
    public static final String TestClass = "mondrian.test.Class";

    /** Retrieves the value of the {@link #TestConnectString} property. */
    public String getTestConnectString() {
        return getProperty(TestConnectString);
    }
    /** Property {@value} */
    public static final String TestConnectString =
                "mondrian.test.connectString";


    // miscellaneous

    /**
     * Retrieves the value of the {@link #JdbcURL} property.
     * The {@link #JdbcURL_Default default value} connects to an ODBC data
     * source.
     */
    public String getFoodmartJdbcURL() {
        return getProperty(JdbcURL, JdbcURL_Default);
    }
    /** Property {@value}. */
    public static final String JdbcURL = "mondrian.foodmart.jdbcURL";
    /** Value is {@value}. */
    public static final String JdbcURL_Default = "jdbc:odbc:MondrianFoodMart";

    /**
     * Retrieves the value of the {@link #LargeDimensionThreshold} property.
     * If a dimension has more than this number of members, use a
     * smart member reader (see {@link mondrian.rolap.SmartMemberReader}).
     * Default is {@link #LargeDimensionThreshold_Default}.
     */
    public int getLargeDimensionThreshold() {
        return getIntProperty(LargeDimensionThreshold, LargeDimensionThreshold_Default);
    }
    /** Property {@value}. */
    public static final String LargeDimensionThreshold =
                "mondrian.rolap.LargeDimensionThreshold";
    /** Value is {@value}. */
    public static final int LargeDimensionThreshold_Default = 100;

    /**
     * Retrieves the value of the {@link #SparseSegmentCountThreshold} property.
     * When storing collections of cell values, we have to choose between a
     * sparse and a dense representation, based upon the <code>possible</code>
     * and <code>actual</code> number of values.
     * The <code>density</code> is <code>actual / possible</code>.
     * We use a sparse representation if
     *   <code>possible -
     *   {@link #getSparseSegmentCountThreshold countThreshold} *
     *   actual &gt;
     *   {@link #getSparseSegmentDensityThreshold densityThreshold}</code>
     *
     * <p>The default values are
     * {@link #SparseSegmentCountThreshold countThreshold} =
     * {@link #SparseSegmentCountThreshold_Default},
     * {@link #SparseSegmentDensityThreshold} =
     * {@link #SparseSegmentDensityThreshold_Default}.
     *
     * <p>At these default values, we use a dense representation
     * for (1000 possible, 0 actual), or (2000 possible, 500 actual), or
     * (3000 possible, 1000 actual). Any fewer actual values, or any more
     * possible values, and we will use a sparse representation.
     */
    public int getSparseSegmentCountThreshold() {
        return getIntProperty(SparseSegmentCountThreshold, SparseSegmentCountThreshold_Default);
    }
    /** Property {@value}. */
    public static final String SparseSegmentCountThreshold =
                "mondrian.rolap.SparseSegmentValueThreshold";
    /** Value is {@value}. */
    public static final int SparseSegmentCountThreshold_Default = 1000;

    /** Retrieves the value of the {@link #SparseSegmentDensityThreshold} property.
     * @see #getSparseSegmentCountThreshold */
    public double getSparseSegmentDensityThreshold() {
        return getDoubleProperty(SparseSegmentDensityThreshold, SparseSegmentDensityThreshold_Default);
    }
    /** Property {@value}. */
    public static final String SparseSegmentDensityThreshold =
                "mondrian.rolap.SparseSegmentDensityThreshold";
    /** Value is {@value}. */
    public static final double SparseSegmentDensityThreshold_Default = 0.5;

    public static final String QueryFilePattern =
                "mondrian.test.QueryFilePattern";
    public String getQueryFilePattern() {
        return getProperty(QueryFilePattern);
    }

    public static final String QueryFileDirectory =
                "mondrian.test.QueryFileDirectory";
    public String getQueryFileDirectory() {
        return getProperty(QueryFileDirectory);
    }

    public static final String Iterations = "mondrian.test.Iterations";
    public static final int Iterations_Default = 1;
    public int getIterations() {
        return getIntProperty(Iterations, Iterations_Default);
    }

    public static final String VUsers = "mondrian.test.VUsers";
    public static final int VUsers_Default = 1;
    public int getVUsers() {
        return getIntProperty(VUsers, VUsers_Default);
    }

    public static final String TimeLimit = "mondrian.test.TimeLimit";
    public static final int TimeLimit_Default = 0;
    /** Returns the time limit for the test run in seconds. If the test is
     * running after that time, it is terminated. */
    public int getTimeLimit() {
        return getIntProperty(TimeLimit, TimeLimit_Default);
    }

    public static final String Warmup = "mondrian.test.Warmup";
    public boolean getWarmup() {
        return getBooleanProperty(Warmup);
    }

    /**
     * Retrieves the URL of the catalog to be used by
     * {@link mondrian.tui.CmdRunner} and XML/A Test.
     */
    public String getCatalogURL() {
        return getProperty("mondrian.catalogURL");
    }
    /**
     * Sets the catalog URL. Writes to {@link System#getProperties()}.
     */
    public void setCatalogURL() {

    }
    /** Property {@value}. */
    public static final String CatalogUrl = "mondrian.catalogURL";

    //////////////////////////////////////////////////////////////////////////
    //
    // properties relating to aggregates
    //
    /**
     * Name of boolean property that controls whether or not aggregates
     * should be used.  If true, then aggregates are used. This property is
     * queried prior to each aggregate query so that changing the value of this
     * property dynamically (not just at startup) is meaningful.
     * Aggregates can be read from the database using the
     * mondrian.rolap.aggregates.Read property but will not be used unless this
     * property is set to true.
     */
    public static final String UseAggregates = "mondrian.rolap.aggregates.Use";

    /**
     * Should aggregates be used.
     *
     * @return true if aggregates are to be used.
     */
    public boolean getUseAggregates() {
        return getBooleanProperty(UseAggregates);
    }

    /**
     * If set to true, then the database is scanned for aggregate tables.
     * Unless mondrian.rolap.aggregates.Use is set to true, the aggregates
     * found will not be used.
     */
    public static final String ReadAggregates 
                    = "mondrian.rolap.aggregates.Read";

    /**
     * Should aggregates be read.
     *
     * @return true if aggregates are to be read.
     */
    public boolean getReadAggregates() {
        return getBooleanProperty(ReadAggregates);
    }


    /**
     * Name of boolean property that controls whether or not aggregate tables
     * are ordered by their volume or row count.
     */
    public static final String ChooseAggregateByVolume
                    = "mondrian.rolap.aggregates.ChooseByVolume";

    /**
     * Should aggregate tables be ordered by volume.
     *
     * @return true if aggregate tables are to be ordered by volume.
     */
    public boolean getChooseAggregateByVolume() {
        return getBooleanProperty(ChooseAggregateByVolume);
    }

    /**
     * This is a String property which can be either a resource in the
     * Mondrian jar or a URL.
     */
    public static final String AggregateRules
                        = "mondrian.rolap.aggregates.rules";

    /**
     * The default value of the AggRules property, a resource in Mondrian jar.
     * DefaultRules.xml is in mondrian.rolap.aggmatcher
     * 
     */
    public static final String AggregateRules_Default = "/DefaultRules.xml";

    /**
     * Get the value of the AggregateRules property which returns the
     * default value, AggregateRules_Default, if not set.
     * <p>
     * Normally, this property is not set by a user.
     *
     * @return
     */
    public String getAggregateRules() {
        return getProperty(AggregateRules, AggregateRules_Default);
    }

    /**
     * This is a String property which is the AggRule element's tag value.
     */
    public static final String AggregateRuleTag
                        = "mondrian.rolap.aggregates.rule.tag";

    /**
     * The default value of the AggRule element tag value.
     */
    public static final String AggregateRuleTag_Default = "default";

    /**
     * Get the value of the AggRule element tag property which returns the
     * default value, AggregateRuleTag_Default, if not set.
     * <p>
     * Normally, this property is not set by a user.
     *
     * @return
     */
    public String getAggregateRuleTag() {
        return getProperty(AggregateRuleTag, AggregateRuleTag_Default);
    }

    /**
     * If set, then as each aggregate request is processed, both the lost
     * and collapsed dimension create and insert sql code is printed.
     * This is for use in the CmdRunner allowing one to create aggregate table
     * generation sql.
     */
    public static final String GenerateAggregateSql
                    = "mondrian.rolap.aggregates.generateSql";

    /**
     * Should aggregate table sql be generated
     *
     * @return true then generate the sql
     */
    public boolean getGenerateAggregateSql() {
        return getBooleanProperty(GenerateAggregateSql);
    }
    //
    //////////////////////////////////////////////////////////////////////////

    /**
     * This is the boolean property that controls whether or not a RolapStar's
     * aggregate data cache is cleared after each query.
     */
    public static final String DisableCaching 
                    = "mondrian.rolap.star.disableCaching";

    /**
     * Returns true means that no RolapStar will cache aggregate data from one
     * query to the next (the cache is cleared after each query).
     *
     * @return
     */
    public boolean getDisableCaching() {
        return getBooleanProperty(DisableCaching);
    }

    /**
     * This is the boolean property that controls whether or not triggers are
     * executed when a MondrianProperty changes.
     */
    public static final String EnableTriggers = "mondrian.olap.triggers.enable";

    /**
     * Returns true means that MondrianProperties Triggers are enabled.
     *
     * @return
     */
    public boolean getEnableTriggers() {
        return getBooleanProperty(EnableTriggers);
    }

    /**
     * This is a boolean property if set to true, the all SqlQuery sql string
     * will be generated in pretty-print mode, formatted for ease of reading.
     */
    public static final String GenerateFormattedSql =
            "mondrian.rolap.generate.formatted.sql";

    /**
     * Returns true mean that all sql string created by the SqlQuery class will
     * be formatted.
     *
     * @return
     */
    public boolean getGenerateFormattedSql() {
        return getBooleanProperty(GenerateFormattedSql);
    }

    /**
     * This is a boolean property if set to true, MDX parsing will be
     * case sensitive.
     */
    public static final String CaseSensitive =
            "mondrian.olap.case.sensitive";

    /**
     * Test if the MDX parser is case sensitive.
     *
     * @return true if MDX should be case sensitive, false otherwise.
     */
    public boolean getCaseSensitive() {
        return getBooleanProperty(CaseSensitive);
    }
}

// End MondrianProperties.java
