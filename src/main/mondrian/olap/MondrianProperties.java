/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 22 December, 2002
*/
package mondrian.olap;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
	private static MondrianProperties properties;

	public static synchronized MondrianProperties instance() {
		if (properties == null) {
			properties = new MondrianProperties();
			// read properties from the file "mondrian.properties", if it
			// exists
			File file = new File("mondrian.properties");
//			System.out.println("looking in " + file.getAbsolutePath());
			if (file.exists()) {
				try {
					properties.load(new FileInputStream(file));
				} catch (IOException e) {
					throw Util.newInternal(e, "while reading from " + file);
				}
			}
			// copy in all system properties which start with "mondrian."
			for (Enumeration keys = System.getProperties().keys();
					keys.hasMoreElements(); ) {
				String key = (String) keys.nextElement();
				String value = System.getProperty(key);
				if (key.startsWith("mondrian.")) {
					properties.setProperty(key, value);
				}
			}
		}
		return properties;
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

	/** Retrieves the value of the {@link #JdbcDrivers} property. */
	public String getJdbcDrivers() {
		return getProperty(JdbcDrivers, "org.hsqldb.jdbcDriver");
	}
	/** Property {@value}. */
	public static final String JdbcDrivers = "mondrian.jdbcDrivers";

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
	 *   {@link #getSparseSegmentCountThreshold countThreshold} &gt;
	 *   actual *
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
