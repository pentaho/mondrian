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

	public int getQueryLimit() {
		return getIntProperty("mondrian.query.limit", 40);
	}

	public int getTraceLevel() {
		return getIntProperty("mondrian.trace.level");
	}

	public String getJdbcDrivers() {
		return getProperty("mondrian.jdbcDrivers", "org.hsqldb.jdbcDriver");
	}

	// mondrian.rolap properties

	public int getCachePoolCostLimit() {
		return getIntProperty("mondrian.rolap.CachePool.costLimit", 10000);
	}

	public boolean getPrintCacheablesAfterQuery() {
		return getBooleanProperty("mondrian.rolap.RolapResult.printCacheables");
	}

	public boolean getFlushAfterQuery() {
		return getBooleanProperty("mondrian.rolap.RolapResult.flushAfterEachQuery");
	}

	// mondrian.test properties

	public String getTestName() {
		return getProperty("mondrian.test.Name");
	}

	public String getTestClass() {
		return getProperty("mondrian.test.Class");
	}

	public String getTestSuite() {
		return getProperty("mondrian.test.Suite");
	}

	public String getTestConnectString() {
		return getProperty("mondrian.test.connectString");
	}

	// miscellaneous

	public String getFoodmartJdbcURL() {
		return getProperty("mondrian.foodmart.jdbcURL",
				"jdbc:hsqldb:demo/hsql/FoodMart");
	}

	/**
	 * If a dimension has more than this number of members, use a
	 * smart member reader (see {@link mondrian.rolap.SmartMemberReader}).
	 * Default is 100.
	 */
	public int getLargeDimensionThreshold() {
		return getIntProperty(LargeDimensionThreshold, 100);
	}
	public static final String LargeDimensionThreshold = "mondrian.rolap.LargeDimensionThreshold";

	/**
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
	 * {@link #getSparseSegmentCountThreshold countThreshold} = 1000,
	 * {@link #getSparseSegmentDensityThreshold} = 0.5.
	 *
	 * <p>At these default values, we use a dense representation
	 * for (1000 possible, 0 actual), or (2000 possible, 500 actual), or
	 * (3000 possible, 1000 actual). Any fewer actual values, or any more
	 * possible values, and we will use a sparse representation.
	 */
	public int getSparseSegmentCountThreshold() {
		return getIntProperty("mondrian.rolap.SparseSegmentValueThreshold", 1000);
	}
	/** @see #getSparseSegmentCountThreshold */
	public double getSparseSegmentDensityThreshold() {
		return getDoubleProperty("mondrian.rolap.SparseSegmentDensityThreshold", 0.5);
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
