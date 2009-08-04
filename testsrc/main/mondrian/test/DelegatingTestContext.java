/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import mondrian.olap.*;

import java.io.PrintWriter;

/**
 * Extension of {@link TestContext} which delegates all behavior to
 * a parent test context.
 *
 * <p>Derived classes can selectively override methods.
 *
 * @author jhyde
 * @since 7 September, 2005
 * @version $Id$
 */
public class DelegatingTestContext extends TestContext {
    protected final TestContext context;

    protected DelegatingTestContext(TestContext context) {
        this.context = context;
    }

    public Util.PropertyList getFoodMartConnectionProperties() {
        return context.getFoodMartConnectionProperties();
    }

    public String getDefaultCubeName() {
        return context.getDefaultCubeName();
    }

    public PrintWriter getWriter() {
        return context.getWriter();
    }
}

// End DelegatingTestContext.java