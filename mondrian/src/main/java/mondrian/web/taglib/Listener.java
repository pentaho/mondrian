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


package mondrian.web.taglib;

import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;

/**
 * <code>Listener</code> creates and destroys a {@link ApplResources} at the
 * appropriate times in the servlet's life-cycle.
 *
 * <p>NOTE: This class must not depend upon any non-standard packages (such as
 * <code>javax.transform</code>) because it is loaded when Tomcat starts, not
 * when the servlet is loaded. (This might be a bug in Tomcat 4.0.3, because
 * it worked in 4.0.1. But anyway.)
 */
public class Listener implements ServletContextListener {

    ApplicationContext applicationContext;

    public Listener() {
    }

    public void contextInitialized(ServletContextEvent event) {
        Class clazz;
        try {
            clazz = Class.forName("mondrian.web.taglib.ApplResources");
        } catch (ClassNotFoundException e) {
            throw new Error(
                "Received [" + e.toString() + "] while initializing servlet");
        }
        Object o = null;
        try {
            o = clazz.newInstance();
        } catch (InstantiationException e) {
            throw new Error(
                "Received [" + e.toString() + "] while initializing servlet");
        } catch (IllegalAccessException e) {
            throw new Error(
                "Received [" + e.toString() + "] while initializing servlet");
        }
        applicationContext = (ApplicationContext) o;
        applicationContext.init(event);
    }

    public void contextDestroyed(ServletContextEvent event) {
        if (applicationContext != null) {
            applicationContext.destroy(event);
        }
    }

    interface ApplicationContext {
        void init(ServletContextEvent event);
        void destroy(ServletContextEvent event);
    }
}

// End Listener.java
