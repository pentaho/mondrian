/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.tui;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import java.util.Properties;
import java.util.Enumeration;
import java.util.Collections;
import java.net.URL;
import java.net.MalformedURLException;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.ServletContext;
import javax.servlet.RequestDispatcher;

/** 
 * This is a partial implementation of the ServletContext where just
 * enough is present to allow for communication between Mondrian's
 * XMLA code and other code in the same JVM.
 * Currently it is used in both the CmdRunner and in XMLA JUnit tests.
 * <p>
 * If you need to add to this implementation, please do so.
 * 
 * @author <a>Richard M. Emberson</a>
 * @version $Id$
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
     *  Returns a URL to the resource that is mapped to a specified path.
     * 
     */
    public URL getResource(String name) throws MalformedURLException {
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

    /**
     * Deprecated. As of Java Servlet API 2.1, with no direct replacement.
     * 
     * This method was originally defined to retrieve a servlet from a
     * ServletContext. In this version, this method always returns null and
     * remains only to preserve binary compatibility. This method will be
     * permanently removed in a future version of the Java Servlet API.
     * 
     * In lieu of this method, servlets can share information using the
     * ServletContext class and can perform shared business logic by invoking
     * methods on common non-servlet classes.
     * 
     * @deprecated Method getServlet is deprecated
     */

    public Servlet getServlet(String s) throws ServletException {
        // TODO
        return null;
    }

    /**
     * Deprecated. As of Java Servlet API 2.0, with no replacement.
     * 
     * This method was originally defined to return an Enumeration of all the
     * servlets known to this servlet context. In this version, this method
     * always returns an empty enumeration and remains only to preserve binary
     * compatibility. This method will be permanently removed in a future
     * version of the Java Servlet API.
     * 
     * @deprecated Method getServlets is deprecated
     * @return  
     */
    public Enumeration getServlets() {
        // TODO
        return null;
    }

    /**
     * Deprecated. As of Java Servlet API 2.1, with no replacement.
     * 
     * This method was originally defined to return an Enumeration of all the
     * servlet names known to this context. In this version, this method always
     * returns an empty Enumeration and remains only to preserve binary
     * compatibility. This method will be permanently removed in a future
     * version of the Java Servlet API.
     * 
     * @deprecated Method getServletNames is deprecated
     * @return  
     */
    public Enumeration getServletNames() {
        // TODO
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
