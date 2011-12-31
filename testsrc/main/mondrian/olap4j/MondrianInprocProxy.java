/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import mondrian.tui.XmlaSupport;

import org.olap4j.driver.xmla.XmlaOlap4jServerInfos;
import org.olap4j.driver.xmla.proxy.XmlaOlap4jProxy;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.*;
import javax.servlet.Servlet;

/**
 * Proxy which implements XMLA requests by talking to mondrian
 * in-process. This is more convenient to debug than an inter-process
 * request using HTTP.
 *
 * @version $Id$
 * @author jhyde
 */
public class MondrianInprocProxy
    implements XmlaOlap4jProxy
{
    private final Map<String, String> catalogNameUrls;
    private final String urlString;
    private final HashMap<List<String>,WeakReference<Servlet>> servletCache =
        new HashMap<List<String>, WeakReference<Servlet>>();

    /**
     * Creates and initializes a MondrianInprocProxy.
     *
     * @param catalogNameUrls Collection of catalog names and the URL where
     * their catalog is to be found. For testing purposes, this should contain
     * a catalog called "FoodMart".
     *
     * @param urlString JDBC connect string; must begin with "jdbc:mondrian:"
     */
    public MondrianInprocProxy(
        Map<String, String> catalogNameUrls,
        String urlString)
    {
        this.catalogNameUrls = catalogNameUrls;
        if (!urlString.startsWith("jdbc:mondrian:")) {
            throw new IllegalArgumentException();
        }
        this.urlString = urlString.substring("jdbc:mondrian:".length());
    }

    // Use single-threaded executor for ease of debugging.
    private static final ExecutorService singleThreadExecutor =
        Executors.newSingleThreadExecutor(
            new ThreadFactory() {
                public Thread newThread(Runnable r) {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    return t;
               }
            }
        );

    public byte[] get(
        XmlaOlap4jServerInfos infos,
        String request)
    {
        try {
            return XmlaSupport.processSoapXmla(
                request, urlString, catalogNameUrls, null, null, servletCache);
        } catch (Exception e) {
            throw new RuntimeException(
                "Error while reading '" + infos.getUrl() + "'", e);
        }
    }

    public Future<byte[]> submit(
        final XmlaOlap4jServerInfos infos,
        final String request)
    {
        return singleThreadExecutor.submit(
            new Callable<byte[]>() {
                public byte[] call() throws Exception {
                    return get(infos, request);
                }
            }
       );
    }

    public String getEncodingCharsetName() {
        return "UTF-8";
    }
}

// End MondrianInprocProxy.java
