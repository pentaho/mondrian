/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import org.xml.sax.SAXException;
import org.olap4j.driver.xmla.proxy.XmlaOlap4jProxy;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import java.lang.ref.WeakReference;
import java.util.concurrent.*;
import java.util.*;
import java.net.URL;
import java.io.IOException;

import mondrian.tui.XmlaSupport;

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
        new InlineExecutorService();

    public byte[] get(URL url, String request) throws IOException {
        try {
            return XmlaSupport.processSoapXmla(
                request, urlString, catalogNameUrls, null, null, servletCache);
        } catch (ServletException e) {
            throw new RuntimeException(
                "Error while reading '" + url + "'", e);
        } catch (SAXException e) {
            throw new RuntimeException(
                "Error while reading '" + url + "'", e);
        }
    }

    public Future<byte[]> submit(
        final URL url,
        final String request)
    {
        return singleThreadExecutor.submit(
            new Callable<byte[]>() {
                public byte[] call() throws Exception {
                    return get(url, request);
                }
            }
       );
    }

    public String getEncodingCharsetName() {
        return "UTF-8";
    }

    /**
     * Trivial executor service that executes all requests using the submitter's
     * thread.
     */
    private static class InlineExecutorService extends AbstractExecutorService {
        private boolean shutdown = false;
        private boolean terminated = false;

        public void shutdown() {
            shutdown = true;
        }

        public List<Runnable> shutdownNow() {
            shutdown = true;
            return Collections.emptyList();
        }

        public boolean isShutdown() {
            return shutdown;
        }

        public boolean isTerminated() {
            return terminated;
        }

        public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException
        {
            terminated = true;
            return true;
        }

        public void execute(Runnable command) {
            command.run();
        }
    }
}

// End MondrianInprocProxy.java
