/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Andreas Voss, 22 March, 2002
*/
package mondrian.web.taglib;

import mondrian.olap.Connection;
import mondrian.olap.DriverManager;
import mondrian.olap.Query;
import mondrian.olap.Result;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionBindingEvent;
import javax.servlet.http.HttpSessionBindingListener;
import javax.xml.parsers.ParserConfigurationException;

/**
 * holds a query/result pair in the users session
 */

public class ResultCache implements HttpSessionBindingListener {
    private static final Logger LOGGER = Logger.getLogger(ResultCache.class);
    private final static String ATTR_NAME = "mondrian.web.taglib.ResultCache.";
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
        String name) {
        String fqname = ATTR_NAME + name;
        ResultCache resultCache = (ResultCache) session.getAttribute(fqname);
        if (resultCache == null) {
            resultCache = new ResultCache(servletContext);
            session.setAttribute(fqname, resultCache);
        }
        return resultCache;
    }

    public void parse(String mdx) {
        query = connection.parseQuery(mdx);
        setDirty();
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
                document = DOMBuilder.build(getResult());
            }
            return document;
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
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
        this.connection =
            DriverManager.getConnection(connectString, servletContext, false);
    }

    /**
     * close connection
     */
    public void valueUnbound(HttpSessionBindingEvent ev) {
        connection.close();
    }


}

// End ResultCache.java
