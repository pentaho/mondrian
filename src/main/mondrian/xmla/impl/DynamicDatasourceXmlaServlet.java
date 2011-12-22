/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla.impl;

import mondrian.server.DynamicContentFinder;
import mondrian.server.RepositoryContentFinder;

/**
 * Extends DefaultXmlaServlet to add dynamic datasource loading capability.
 *
 * @author Thiyagu Ajit
 * @author Luc Boudreau
 * @version $Id$
 */
public class DynamicDatasourceXmlaServlet extends MondrianXmlaServlet {
    protected RepositoryContentFinder makeContentFinder(String dataSources) {
        return new DynamicContentFinder(dataSources);
    }
    @Override
    public void destroy() {
        super.destroy();
    }
}

// End DynamicDatasourceXmlaServlet.java
