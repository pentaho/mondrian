/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

import mondrian.resource.MondrianResource;

import org.apache.log4j.Logger;

/**
 * <code>ConnectionBase</code> implements some of the methods in
 * {@link Connection}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 */
public abstract class ConnectionBase implements Connection {

    public static void memoryUsageNotification(Query query, String msg) {
        query.setOutOfMemory(msg);
    }

    protected ConnectionBase() {
    }

    protected abstract Logger getLogger();


    public String getFullConnectString() {
        String s = getConnectString();
        String catalogName = getCatalogName();
        if (catalogName != null) {
            int len = s.length() + catalogName.length() + 32;
            StringBuilder buf = new StringBuilder(len);
            buf.append(s);
            if (!s.endsWith(";")) {
                buf.append(';');
            }
            buf.append("Initial Catalog=");
            buf.append(catalogName);
            buf.append(';');
            s = buf.toString();
        }
        return s;
    }

    public Query parseQuery(String query) {
        return parseQuery(query, null, false, false);
    }

    public Query parseQuery(String query, boolean load) {
        return parseQuery(query, null, load, false);
    }

    /**
     * Parses a query, with specified function table and the mode for strict
     * validation(if true then invalid members are not ignored).
     *
     * <p>This method is only used in testing and by clients that need to
     * support customized parser behavior. That is why this method is not part
     * of the Connection interface.
     *
     * @param query MDX query that requires special parsing
     * @param funTable Customized function table to use in parsing
     * @param strictValidation If true, do not ignore invalid members
     * @return Query the corresponding Query object if parsing is successful
     * @throws MondrianException if parsing fails
     * @see mondrian.olap.CustomizedParserTest
     */
    public Query parseQuery(String query, FunTable funTable,
        boolean strictValidation) {
        return parseQuery(query, funTable, false, strictValidation);
    }

    public Exp parseExpression(String expr) {
        boolean debug = false;
        if (getLogger().isDebugEnabled()) {
            //debug = true;
            StringBuilder buf = new StringBuilder(256);
            buf.append(Util.nl);
            buf.append(expr);
            getLogger().debug(buf.toString());
        }
        try {
            Parser parser = new Parser();
            final FunTable funTable = getSchema().getFunTable();
            Exp q = parser.parseExpression(this, expr, debug, funTable);
            return q;
        } catch (Throwable exception) {
            throw
                MondrianResource.instance().FailedToParseQuery.ex(
                    expr,
                    exception);
        }
    }

    private Query parseQuery(String query, FunTable cftab, boolean load,
        boolean strictValidation) {
        Parser parser = new Parser();
        boolean debug = false;
        final FunTable funTable;

        if (cftab == null) {
            funTable = getSchema().getFunTable();
        } else {
            funTable = cftab;
        }

        if (getLogger().isDebugEnabled()) {
            //debug = true;
            StringBuilder buf = new StringBuilder(256);
            buf.append(Util.nl);
            buf.append(query);
            getLogger().debug(buf.toString());
        }

        try {
            Query q =
                parser.parseInternal(this, query, debug, funTable, load,
                    strictValidation);
            return q;
        } catch (Throwable e) {
            throw MondrianResource.instance().FailedToParseQuery.ex(query, e);
        }
    }
}

// End ConnectionBase.java
