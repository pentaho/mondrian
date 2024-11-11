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


package mondrian.test;

import mondrian.olap.Util;

import java.io.PrintWriter;

/**
 * Extension of {@link TestContext} which delegates all behavior to
 * a parent test context.
 *
 * <p>Derived classes can selectively override methods.
 *
 * @author jhyde
 * @since 7 September, 2005
 */
public class DelegatingTestContext extends TestContext {
    protected final TestContext context;

    protected DelegatingTestContext(TestContext context) {
        this.context = context;
    }

    public Util.PropertyList getConnectionProperties() {
        return context.getConnectionProperties();
    }

    public String getDefaultCubeName() {
        return context.getDefaultCubeName();
    }

    public PrintWriter getWriter() {
        return context.getWriter();
    }
}

// End DelegatingTestContext.java
