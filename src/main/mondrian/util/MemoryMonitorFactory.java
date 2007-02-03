/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 23, 2002
*/
package mondrian.util;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;

/** 
 * The <code>MemoryMonitorFactory</code> is used to get the application's
 * <code>MemoryMonitor</code>. The <code>MemoryMonitorFactory</code> is
 * based upon the <code>ObjectFactory</code> generic. The 
 * <code>MemoryMonitorFactory</code> implementation has a single, default
 * <code>MemoryMonitor</code> per JVM instance which can be overridden
 * using the provided <code>ThreadLocal</code> variable. Normally, this
 * <code>ThreadLocal</code> override should only be used during JUnit testing.
 * The JUnit test, set the <code>ThreadLocal</code> variable to the name of its 
 * own implementation of the <code>MemoryMonitor</code> interface and
 * then calls the <code>ObjectFactory</code> <code>getObject</code> method.
 * After doing the test, the <code>ThreadLocal</code> variable should
 * be cleared. While the <code>ObjectFactory</code> permits the use
 * of <code>System</code> properties to provide a class name to the
 * factory, such usage is not thread-safe while using the
 * <code>ThreadLocal</code> is thread-safe.
 * 
 * @author <a>Richard M. Emberson</a>
 * @since Feb 03 2007
 * @version $Id$
 */
public final class MemoryMonitorFactory extends ObjectFactory<MemoryMonitor> {

    /** 
     * Single instance of the <code>MemoryMonitorFactory</code>. 
     */
    private static final MemoryMonitorFactory factory;
    static {
        factory = new MemoryMonitorFactory();
    }

    /** 
     * Access the <code>MemoryMonitorFactory</code> instance. 
     * 
     * @return the <code>MemoryMonitorFactory</code>.
     */
    public static MemoryMonitorFactory instance() {
        return factory;
    }

    /** 
     * ThreadLocal used to hold the class name of an <code>MemoryMonitor</code> 
     * implementation. Generally, this should only be used for testing.
     */
    private static final ThreadLocal ClassName = new ThreadLocal();

    /** 
     * Get the class name of a <code>MemoryMonitor</code> implementation
     * or null.
     * 
     * @return the class name or null.
     */
    private static String getThreadLocalClassName() {
        return (String) ClassName.get();
    }

    /** 
     * Set the class name of a  <code>MemoryMonitor</code> implementation. 
     * This should be called (obviously) before calling the
     * <code>MemoryMonitorFactory</code> <code>getObject</code> 
     * method to get the <code>MemoryMonitor</code> implementation.
     * Generally, this is only used for testing.
     * 
     * @param className 
     */
    public static void setThreadLocalClassName(String className) {
        ClassName.set(className);
    }

    // put if finally clause
    
    /** 
     * Clear the class name (regardless of whether a class name was set). 
     * When a class name is set using <code>setThreadLocalClassName</code>,
     * the setting whould be done in a try-block and a call to this
     * clear method should be in the finally-clause of that try-block.
     */
    public static void clearThreadLocalClassName() {
        ClassName.set(null);
    }

    /** 
     * The <code>MemoryMonitorFactory</code>'s default
     * <code>MemoryMonitor</code> implementation.
     */
    private static MemoryMonitor defaultMemoryMonitor;

    /** 
     * The constructor for the <code>MemoryMonitorFactory</code>. This passes 
     * the <code>MemoryMonitor</code> class to the <code>ObjectFactory</code>
     * base class.
     */
    private MemoryMonitorFactory() {
        super(MemoryMonitor.class);
    }

    /** 
     * Is the use of a <code>MemoryMonitor</code> enabled. 
     * 
     * @return <code>true</code> if enabled and <code>false</code> otherwise.
     */
    protected boolean enabled() {
        return MondrianProperties.instance().MemoryMonitor.get();
    }

    /** 
     * Get the class name set in the <code>ThreadLocal</code> or null. 
     * 
     * @return class name or null.
     */
    protected String getClassName() {
        return getThreadLocalClassName();
    }

    /** 
     * The <code>MemoryMonitorFactory</code>'s implementation of the
     * <code>ObjectFactory</code>'s abstract method which returns
     * the default <code>MemoryMonitor</code> instance.
     * For Java4 or if the <code>MemoryMonitorFactory</code> is not enabled
     * then this method returns the "faux" <code>MemoryMonitor</code>
     * implementation, it does nothing. When enabled and for
     * Java5 and above JVMs, and instance of the 
     * <code>NotificationMemoryMonitor</code> is returned.
     * 
     * @param parameterTypes  not used
     * @param parameterValues  not used
     * @return <code>MemoryMonitor</code> instance 
     * @throws CreationException if the <code>MemoryMonitor</code> can not be 
     * created.
     */
    protected MemoryMonitor getDefault(Class[] parameterTypes,
                                       Object[] parameterValues)
            throws CreationException {
        if (defaultMemoryMonitor == null) {
            if (! enabled() || Util.PreJdk15) {
                // not enabled or Java4 or below
                defaultMemoryMonitor = new FauxMemoryMonitor();
            } else {
                // enabled and Java5 or above
                defaultMemoryMonitor = new NotificationMemoryMonitor();
            }
        }
        return defaultMemoryMonitor;
    }

}
