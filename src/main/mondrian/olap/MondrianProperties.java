/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 22 December, 2002
*/
package mondrian.olap;

import javax.servlet.ServletContext;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

/**
 * <code>MondrianProperties</code> contains the properties which determine the
 * behavior of a mondrian instance.
 *
 * There is a method for property valid in a <code>mondrian.properties</code>
 * file. Although it is possible to retrieve properties using the inherited
 * {@link Properties#getProperty(String)} method, we recommend that you use
 * methods in this class.
 *
 * @author jhyde
 * @since 22 December, 2002
 * @version $Id$
 **/
public class MondrianProperties extends PropertiesPlus {
	/**
	 * Properties, drawn from {@link System#getProperties}, plus the contents
	 * of "mondrian.properties" if it exists. A singleton.
	 */
	private static MondrianProperties instance;
    private static final String mondrianDotProperties = "mondrian.properties";
    private static final String buildDotProperties = "build.properties";
    private int populateCount;

    public static synchronized MondrianProperties instance() {
		if (instance == null) {
			instance = new MondrianProperties();
			instance.populate(null);
		}
		return instance;
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
                System.out.println("Mondrian: file '" + file.getAbsolutePath() +
                        "' could not be loaded (" + e + ")");
            }
        }
        /*
        else if (populateCount == 0) {
            System.out.println("Mondrian: Warning: file '" +
                    file.getAbsolutePath() + "' not found");
        }
        */
		// If we're in a servlet, read "mondrian.properties" from JAR file.
		if (servletContext != null) {
			try {
				final URL resourceUrl = servletContext.getResource("/" + mondrianDotProperties);
				if (resourceUrl != null) {
					load(resourceUrl);
				}
				/*
				else if (populateCount == 0) {
                    System.out.println("Mondrian: Warning: servlet resource '" +
                            mondrianDotProperties + "' not found");
                }
                */
			} catch (MalformedURLException e) {
				System.out.println("Mondrian: '" + mondrianDotProperties +
                        "' could not be loaded from servlet context (" + e +
                        ")");
			}
		}
		// copy in all system properties which start with "mondrian."
		int count = 0;
		for (Enumeration keys = System.getProperties().keys();
				keys.hasMoreElements(); ) {
			String key = (String) keys.nextElement();
			String value = System.getProperty(key);
			if (key.startsWith("mondrian.")) {
				setProperty(key, value);
				count++;
			}
		}
        if (populateCount++ == 0) {
    		System.out.println("Mondrian: loaded " + count + " system properties");
        }
	}

	/** Tries to load properties from a URL. Does not fail, just prints success
	 * or failure to {@link System#out}. */
	private void load(final URL url) {
		try {
			load(url.openStream());
            if (populateCount == 0) {
    			System.out.println("Mondrian: properties loaded from '" + url + "'");
            }
		} catch (IOException e) {
			System.out.println("Mondrian: error while loading properties from '" + url + "' (" + e + ")");
		}
	}

	/** Retrieves the value of the {@link #QueryLimit} property,
	 * default value {@link #QueryLimit_Default}. */
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
	public static final String JdbcDrivers_Default = "sun.jdbc.odbc.JdbcOdbcDriver," +
			"org.hsqldb.jdbcDriver," +
			"oracle.jdbc.OracleDriver," +
			"com.mysql.jdbc.Driver";

	/** Retrieves the value of the {@link #ResultLimit} property,
	 */
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
	public static final String PrintCacheablesAfterQuery = "mondrian.rolap.RolapResult.printCacheables";

	/** Retrieves the value of the {@link #FlushAfterQuery} property. */
	public boolean getFlushAfterQuery() {
		return getBooleanProperty(FlushAfterQuery);
	}
	/** Property {@value}. */
	public static final String FlushAfterQuery = "mondrian.rolap.RolapResult.flushAfterEachQuery";

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

	/** Retreives the value of the {@link #TestConnectString} property. */
	public String getTestConnectString() {
		return getProperty(TestConnectString);
	}
	/** Property {@value} */
	public static final String TestConnectString = "mondrian.test.connectString";


	// miscellaneous

	/** Retrieves the value of the {@link #JdbcURL} property. */
	public String getFoodmartJdbcURL() {
		return getProperty(JdbcURL, "jdbc:hsqldb:demo/hsql/FoodMart");
	}
	/** Property {@value}. */
	public static final String JdbcURL = "mondrian.foodmart.jdbcURL";

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
	public static final String LargeDimensionThreshold = "mondrian.rolap.LargeDimensionThreshold";
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
	public static final String SparseSegmentCountThreshold = "mondrian.rolap.SparseSegmentValueThreshold";
	/** Value is {@value}. */
	public static final int SparseSegmentCountThreshold_Default = 1000;

	/** Retrieves the value of the {@link #SparseSegmentDensityThreshold} property.
	 * @see #getSparseSegmentCountThreshold */
	public double getSparseSegmentDensityThreshold() {
		return getDoubleProperty(SparseSegmentDensityThreshold, SparseSegmentDensityThreshold_Default);
	}
	/** Property {@value}. */
	public static final String SparseSegmentDensityThreshold = "mondrian.rolap.SparseSegmentDensityThreshold";
	/** Value is {@value}. */
	public static final double SparseSegmentDensityThreshold_Default = 0.5;

    public static final String QueryFilePattern = "mondrian.test.QueryFilePattern";
    public String getQueryFilePattern() {
        return getProperty(QueryFilePattern);
    }

    public static final String QueryFileDirectory = "mondrian.test.QueryFileDirectory";
    public String getQueryFileDirectory() {
        return getProperty(QueryFileDirectory);
    }

    public static final String CachePoolType = "mondrian.rolap.CachePoolType";
    public static final String CachePoolType_Soft = "soft";
    public static final String CachePoolType_Mondrian = "mondrian";
    public static final String CachePoolType_Hard = "hard";

    /**
     * The cache pool type. Legal values are {@link #CachePoolType_Soft},
     * {@link #CachePoolType_Hard} and {@link #CachePoolType_Mondrian}.
     * <p>
     * A <code>CachePoolType_Soft</code> cache pool uses a soft reference HashMap for the
     * cache pool. Its behavior is driven by the Java garbage collector. See
     * {@link mondrian.rolap.cache.SoftCachePool} for more information.
     * <p>
     * The <code>CachePoolType_Hard</code> keeps cache entries in memory until
     * the cache is flushed. See {@link mondrian.rolap.cache.HardCachePool} for
     * more information.
     * <p>
     * The <code>CachePoolType_Mondrian</code> uses a cost-based metric for keeping
     * cache entries in the pool. See {@link mondrian.rolap.cache.MondrianCachePool} for
     * more information.
     */
    public String getCachePoolType() {
        return getProperty(CachePoolType, CachePoolType_Soft);
    }
}

/**
 * <code>PropertiesPlus</code> adds a couple of convenience methods to
 * {@link java.util.Properties}.
 **/
class PropertiesPlus extends Properties {
	/**
	 * Retrieves an integer property. Returns -1 if the property is not
	 * found, or if its value is not an integer.
	 */
	public int getIntProperty(String key) {
		return getIntProperty(key, -1);
	}
	/**
	 * Retrieves an integer property. Returns <code>default</code> if the
	 * property is not found.
	 */
	public int getIntProperty(String key, int defaultValue) {
		String value = getProperty(key);
		if (value == null) {
			return defaultValue;
		}
		int i = Integer.valueOf(value).intValue();
		return i;
	}
	/**
	 * Retrieves a double-precision property. Returns <code>default</code> if
	 * the property is not found.
	 */
	public double getDoubleProperty(String key, double defaultValue) {
		String value = getProperty(key);
		if (value == null) {
			return defaultValue;
		}
		double d = Double.valueOf(value).doubleValue();
		return d;
	}
	/**
	 * Retrieves a boolean property. Returns <code>true</code> if the
	 * property exists, and its value is <code>1</code>, <code>true</code>
	 * or <code>yes</code>; returns <code>false</code> otherwise.
	 */
	public boolean getBooleanProperty(String key) {
		String value = getProperty(key);
		if (value == null) {
			return false;
		}
		return value.equalsIgnoreCase("1") ||
			value.equalsIgnoreCase("true") ||
			value.equalsIgnoreCase("yes");
	}
}

// End MondrianProperties.java
