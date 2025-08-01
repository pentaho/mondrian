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

import java.util.*;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletContext;

/**
 * This is a partial implementation of the ServletConfig where just
 * enough is present to allow for communication between Mondrian's
 * XMLA code and other code in the same JVM.
 * Currently it is used in both the CmdRunner and in XMLA JUnit tests.
 * <p>
 * If you need to add to this implementation, please do so.
 *
 * @author <a>Richard M. Emberson</a>
 */
public class MockServletConfig implements ServletConfig {
    private String servletName;
    private Map<String, String> initParams;
    private ServletContext servletContext;

    public MockServletConfig() {
        this(null);
    }
    public MockServletConfig(ServletContext servletContext) {
        this.initParams = new HashMap<String, String>();
        this.servletContext = servletContext;
    }

    /**
     * Returns the name of this servlet instance.
     *
     */
    public String getServletName() {
        return servletName;
    }

    /**
     * Returns a reference to the ServletContext in which the servlet is
     * executing.
     *
     */
    public ServletContext getServletContext() {
        return servletContext;
    }

    /**
     * Returns a String containing the value of the named initialization
     * parameter, or null if the parameter does not exist.
     *
     */
    public String getInitParameter(String key) {
        return initParams.get(key);
    }

    /**
     *  Returns the names of the servlet's initialization parameters as an
     *  Enumeration of String objects, or an empty Enumeration if the servlet
     *  has no initialization parameters.
     *
     */
    public Enumeration getInitParameterNames() {
        return Collections.enumeration(initParams.keySet());
    }

    /////////////////////////////////////////////////////////////////////////
    //
    // implementation access
    //
    /////////////////////////////////////////////////////////////////////////
    public void setServletName(String servletName) {
        this.servletName = servletName;
    }
    public void addInitParameter(String key, String value) {
        if (value != null) {
            this.initParams.put(key, value);
        }
    }
    public void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
    }
}

// End MockServletConfig.java
