/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.tui;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import jakarta.servlet.ServletContext;
import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.Servlet;
import jakarta.servlet.ServletException;
import jakarta.servlet.SessionCookieConfig;
import jakarta.servlet.SessionTrackingMode;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterRegistration;
import jakarta.servlet.ServletRegistration;
import jakarta.servlet.descriptor.JspConfigDescriptor;

/**
 * Partial implementation of the {@link ServletContext} where just
 * enough is present to allow for communication between Mondrian's
 * XMLA code and other code in the same JVM.
 *
 * <p>Currently it is used in both the CmdRunner and in XMLA JUnit tests.
 * If you need to add to this implementation, please do so.
 *
 * @author Richard M. Emberson
 */
public class MockServletContext implements ServletContext {

    public static final String PARAM_DATASOURCES_CONFIG = "DataSourcesConfig";
    public static final String PARAM_CHAR_ENCODING = "CharacterEncoding";
    public static final String PARAM_CALLBACKS = "Callbacks";

    private Map<String, URL> resources;
    private Map<String, Object> attributes;
    private int majorVersion;
    private int minorVersion;
    private Properties parameters;

    public MockServletContext() {
        this.majorVersion = 1;
        this.minorVersion = 1;
        this.resources = Collections.emptyMap();
        this.attributes = Collections.emptyMap();
        this.parameters = new Properties();
    }


    @Override
    public String getContextPath() {
        return "";
    }

    /**
     * Returns a ServletContext object that corresponds to a specified URL on
     * the server.
     *
     */
    public ServletContext getContext(String s) {
        // TODO
        return null;
    }

    /**
     * Returns the major version of the Java Servlet API that this servlet
     * container supports.
     *
     */
    public int getMajorVersion() {
        return this.majorVersion;
    }

    /**
     * Returns the minor version of the Servlet API that this servlet container
     * supports.
     *
     */
    public int getMinorVersion() {
        return this.minorVersion;
    }

    @Override
    public int getEffectiveMajorVersion() {
        return 0;
    }

    @Override
    public int getEffectiveMinorVersion() {
        return 0;
    }

    /**
     * Returns the MIME type of the specified file, or null if the MIME type is
     * not known.
     *
     */
    public String getMimeType(String s) {
        // TODO
        return null;
    }

    /**
     *
     *
     */
    public Set getResourcePaths(String s) {
        // TODO
        return null;
    }

    /**
     * Returns a URL to the resource that is mapped to a specified path.
     */
    public URL getResource(String name) throws MalformedURLException {
        if (!resources.containsKey(name)) {
            addResource(name, new URL("file://" + name));
        }
        return resources.get(name);
    }

    /**
     *  Returns the resource located at the named path as an InputStream object.
     *
     */
    public InputStream getResourceAsStream(String s) {
        // TODO
        return null;
    }

    /**
     *  Returns a RequestDispatcher object that acts as a wrapper for the
     *  resource located at the given path.
     *
     */
    public RequestDispatcher getRequestDispatcher(String s) {
        // TODO
        return null;
    }

    /**
     * Returns a RequestDispatcher object that acts as a wrapper for the named
     * servlet.
     *
     */
    public RequestDispatcher getNamedDispatcher(String s) {
        // TODO
        return null;
    }

    public Servlet getServlet(String s) throws ServletException {
        // method is deprecated as of Servlet API 2.1
        return null;
    }

    public Enumeration getServlets() {
        // method is deprecated as of Servlet API 2.1
        return null;
    }

    public Enumeration getServletNames() {
        // method is deprecated as of Servlet API 2.1
        return null;
    }

    /**
     * Writes the specified message to a servlet log file, usually an event log.
     *
     */
    public void log(String s) {
        // TODO
    }

    /**
     * Deprecated. As of Java Servlet API 2.1, use log(String message, Throwable
     * throwable) instead.
     *
     * This method was originally defined to write an exception's stack trace
     * and an explanatory error message to the servlet log file.
     *
     * @deprecated Method log is deprecated
     */
    public void log(Exception exception, String s) {
        log(s, exception);
    }

    /**
     *  Writes an explanatory message and a stack trace for a given Throwable
     *  exception to the servlet log file.
     *
     */
    public void log(String s, Throwable throwable) {
        // TODO
    }

    /**
     * Returns a String containing the real path for a given virtual path.
     *
     */
    public String getRealPath(String path) {
        return path;
    }

    /**
     * Returns the name and version of the servlet container on which the
     * servlet is running.
     *
     */
    public String getServerInfo() {
        // TODO
        return null;
    }

    /**
     * Returns a String containing the value of the named context-wide
     * initialization parameter, or null if the parameter does not exist.
     *
     */
    public String getInitParameter(String name) {
        return parameters.getProperty(name);
    }

    /**
     * Returns the names of the context's initialization parameters as an
     * Enumeration of String objects, or an empty Enumeration if the context has
     * no initialization parameters.
     *
     */
    public Enumeration getInitParameterNames() {
        return parameters.propertyNames();
    }

    @Override
    public boolean setInitParameter( String s, String s1 ) {
        return false;
    }

    /**
     *
     *
     */
    public Object getAttribute(String s) {
        return this.attributes.get(s);
    }

    /**
     * Returns an Enumeration containing the attribute names available within
     * this servlet context.
     *
     */
    public Enumeration getAttributeNames() {
        // TODO
        return Collections.enumeration(this.attributes.keySet());
    }

    /**
     *  Binds an object to a given attribute name in this servlet context.
     *
     */
    public void setAttribute(String s, Object obj) {
        if (this.attributes == Collections.EMPTY_MAP) {
            this.attributes = new HashMap<String, Object>();
        }
        this.attributes.put(s, obj);
    }

    /**
     *  Removes the attribute with the given name from the servlet context.
     *
     */
    public void removeAttribute(String s) {
        this.attributes.remove(s);
    }

    /**
     *
     *
     */
    public String getServletContextName() {
        // TODO
        return null;
    }

    @Override
    public ServletRegistration.Dynamic addServlet( String s, String s1 ) {
        return null;
    }

    @Override
    public ServletRegistration.Dynamic addServlet( String s, Servlet servlet ) {
        return null;
    }

    @Override
    public ServletRegistration.Dynamic addServlet( String s, Class<? extends Servlet> aClass ) {
        return null;
    }

    @Override
    public ServletRegistration.Dynamic addJspFile( String s, String s1 ) {
        return null;
    }

    @Override
    public <T extends Servlet> T createServlet( Class<T> aClass ) throws ServletException {
        return null;
    }

    @Override
    public ServletRegistration getServletRegistration( String s ) {
        return null;
    }

    @Override
    public Map<String, ? extends ServletRegistration> getServletRegistrations() {
        return Map.of();
    }

    @Override
    public FilterRegistration.Dynamic addFilter( String s, String s1 ) {
        return null;
    }

    @Override
    public FilterRegistration.Dynamic addFilter( String s, Filter filter ) {
        return null;
    }

    @Override
    public FilterRegistration.Dynamic addFilter( String s, Class<? extends Filter> aClass ) {
        return null;
    }

    @Override
    public <T extends Filter> T createFilter( Class<T> aClass ) throws ServletException {
        return null;
    }

    @Override
    public FilterRegistration getFilterRegistration( String s ) {
        return null;
    }

    @Override
    public Map<String, ? extends FilterRegistration> getFilterRegistrations() {
        return Map.of();
    }

    @Override
    public SessionCookieConfig getSessionCookieConfig() {
        return null;
    }

    @Override
    public void setSessionTrackingModes( Set<SessionTrackingMode> set ) {

    }

    @Override
    public Set<SessionTrackingMode> getDefaultSessionTrackingModes() {
        return Set.of();
    }

    @Override
    public Set<SessionTrackingMode> getEffectiveSessionTrackingModes() {
        return Set.of();
    }

    @Override
    public void addListener(String s) {

    }

    @Override
    public <T extends EventListener> void addListener(T t) {

    }

    @Override
    public void addListener( Class<? extends EventListener> aClass ) {

    }

    @Override
    public <T extends EventListener> T createListener( Class<T> aClass ) throws ServletException {
        return null;
    }

    @Override
    public JspConfigDescriptor getJspConfigDescriptor() {
        return null;
    }

    @Override
    public ClassLoader getClassLoader() {
        return null;
    }

    @Override
    public void declareRoles( String... strings ) {

    }

    @Override
    public String getVirtualServerName() {
        return "";
    }

    @Override
    public int getSessionTimeout() {
        return 0;
    }

    @Override
    public void setSessionTimeout( int i ) {

    }

    @Override
    public String getRequestCharacterEncoding() {
        return "";
    }

    @Override
    public void setRequestCharacterEncoding( String s ) {

    }

    @Override
    public String getResponseCharacterEncoding() {
        return "";
    }

    @Override
    public void setResponseCharacterEncoding( String s ) {

    }


    /////////////////////////////////////////////////////////////////////////
    //
    // implementation access
    //
    /////////////////////////////////////////////////////////////////////////
    public void setMajorVersion(int majorVersion) {
        this.majorVersion = majorVersion;
    }
    public void setMinorVersion(int minorVersion) {
        this.minorVersion = minorVersion;
    }
    public void addResource(String name, URL url) {
        if (this.resources == Collections.EMPTY_MAP) {
            this.resources = new HashMap<String, URL>();
        }
        this.resources.put(name, url);
    }
    public void addInitParameter(String name, String value) {
        if (value != null) {
            this.parameters.setProperty(name, value);
        }
    }
}

// End MockServletContext.java
