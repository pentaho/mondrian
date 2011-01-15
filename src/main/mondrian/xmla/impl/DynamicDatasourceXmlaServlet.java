/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.xmla.impl;

import mondrian.olap.*;
import mondrian.server.DynamicContentFinder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Extends DefaultXmlaServlet to add dynamic datasource loading capability.
 * Limitations : Catalog name should be unique across the datasources
 *
 * <p>The schema is updated every X milliseconds for each request to
 * {@link DynamicDatasourceXmlaServlet#doPost(HttpServletRequest,
 * HttpServletResponse)}, where X is
 * {@link MondrianProperties#XmlaSchemaRefreshInterval}
 *
 * @author Thiyagu Ajit, Luc Boudreau
 */
public class DynamicDatasourceXmlaServlet extends DefaultXmlaServlet {

    @Override
    protected DynamicContentFinder makeContentFinder(String dataSources) {
        final int refreshIntervalMillis =
            MondrianProperties.instance().XmlaSchemaRefreshInterval.get();
        return new DynamicContentFinder(dataSources, refreshIntervalMillis);
    }

    protected void doPost(
        HttpServletRequest request,
        HttpServletResponse response)
        throws ServletException, IOException
    {
        ((DynamicContentFinder) contentFinder).check();
        super.doPost(request, response);
    }

    /**
     * Checks for updates to datasources content, flushes obsolete catalogs.
     *
     * @deprecated Use {@link DynamicContentFinder#reloadDataSources()}
     */
    void reloadDataSources() {
        ((DynamicContentFinder) contentFinder).reloadDataSources();
    }
}

// End DynamicDatasourceXmlaServlet.java
