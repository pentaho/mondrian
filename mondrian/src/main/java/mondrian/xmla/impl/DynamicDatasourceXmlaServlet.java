/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.xmla.impl;

import mondrian.server.DynamicContentFinder;
import mondrian.server.RepositoryContentFinder;

import java.util.HashMap;
import java.util.Map;

/**
 * Extends DefaultXmlaServlet to add dynamic datasource loading capability.
 *
 * @author Thiyagu Ajit
 * @author Luc Boudreau
 */
public class DynamicDatasourceXmlaServlet extends MondrianXmlaServlet {
    private static final long serialVersionUID = 1L;

    /**
     * A map of datasources definitions to {@link DynamicContentFinder}
     * instances.
     */
    private final Map<String, DynamicContentFinder> finders =
        new HashMap<String, DynamicContentFinder>();

    @Override
    protected RepositoryContentFinder makeContentFinder(String dataSources) {
        if (!finders.containsKey(dataSources)) {
            finders.put(dataSources, new DynamicContentFinder(dataSources));
        }
        return finders.get(dataSources);
    }
    @Override
    public void destroy() {
        for (DynamicContentFinder finder : finders.values()) {
            finder.shutdown();
        }
        super.destroy();
    }
}

// End DynamicDatasourceXmlaServlet.java
