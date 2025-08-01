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

import mondrian.olap.*;
import mondrian.spi.impl.ServletContextCatalogLocator;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.w3c.dom.Document;

import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpSession;
import jakarta.servlet.http.HttpSessionBindingEvent;
import jakarta.servlet.http.HttpSessionBindingListener;
import javax.xml.parsers.ParserConfigurationException;

/**
 * Holds a query/result pair in the user's session.
 *
 * @author Andreas Voss, 22 March, 2002
 */
public class ResultCache implements HttpSessionBindingListener {
    private static final Logger LOGGER = LogManager.getLogger(ResultCache.class);
    private static final String ATTR_NAME = "mondrian.web.taglib.ResultCache.";
    private Query query = null;
    private Result result = null;
    private Document document = null;
    private ServletContext servletContext;
    private Connection connection;

    private ResultCache(ServletContext context) {
        this.servletContext = context;
    }


    /**
     * Retrieves a cached query. It is identified by its name and the
     * current session. The servletContext parameter is necessary because
     * HttpSession.getServletContext was not added until J2EE 1.3.
     */
    public static ResultCache getInstance(
        HttpSession session,
        ServletContext servletContext,
        String name)
    {
        String fqname = ATTR_NAME + name;
        ResultCache resultCache = (ResultCache) session.getAttribute(fqname);
        if (resultCache == null) {
            resultCache = new ResultCache(servletContext);
            session.setAttribute(fqname, resultCache);
        }
        return resultCache;
    }

    public void parse(String mdx) {
        if (connection != null) {
            query = connection.parseQuery(mdx);
            setDirty();
        } else {
            LOGGER.error("null connection");
        }
    }

    public Result getResult() {
        if (result == null) {
            long t1 = System.currentTimeMillis();
            result = connection.execute(query);
            long t2 = System.currentTimeMillis();
            LOGGER.debug(
                "Execute query took " + (t2 - t1) + " millisec");
        }
        return result;
    }

    public Document getDOM() {
        try {
            if (document == null) {
                document = DomBuilder.build(getResult());
            }
            return document;
        } catch (ParserConfigurationException e) {
            LOGGER.error(e);
            throw new RuntimeException(e.toString());
        }
    }

    /**
     * Returns the {@link Query}. If you modify the query, call
     * <code>{@link #setDirty}(true)</code>.
     */
    public Query getQuery() {
        return query;
    }

    /**
     * Sets the query. Automatically calls <code>{@link #setDirty}(true)</code>.
     */
    public void setQuery(Query query) {
        this.query = query;
        setDirty();
    }
    /**
     * set to dirty after you have modified the query to force a recalcuation
     */
    public void setDirty() {
        result = null;
        document = null;
    }

    /**
     * create a new connection to Mondrian
     */
    public void valueBound(HttpSessionBindingEvent ev) {
        String connectString =
            servletContext.getInitParameter("connectString");
        LOGGER.debug("connectString: " + connectString);
        this.connection =
            DriverManager.getConnection(
                connectString,
                new ServletContextCatalogLocator(servletContext));
        if (this.connection == null) {
            throw new RuntimeException(
                "No ROLAP connection from connectString: "
                    + connectString);
        }
    }

    /**
     * close connection
     */
    public void valueUnbound(HttpSessionBindingEvent ev) {
        if (connection != null) {
            connection.close();
        }
    }


}

// End ResultCache.java
