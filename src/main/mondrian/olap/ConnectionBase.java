/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2010 Julian Hyde and others
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

    public QueryPart parseStatement(String query) {
        return parseStatement(query, null, false);
    }

    public Query parseQuery(String query) {
        return (Query) parseStatement(query);
    }

    public Query parseQuery(String query, boolean load) {
        return (Query) parseStatement(query, null, false);
    }

    /**
     * Parses a query, with specified function table and the mode for strict
     * validation(if true then invalid members are not ignored).
     *
     * <p>This method is only used in testing and by clients that need to
     * support customized parser behavior. That is why this method is not part
     * of the Connection interface.
     *
     * <p>See test case mondrian.olap.CustomizedParserTest.
     *
     * @param query MDX query that requires special parsing
     * @param funTable Customized function table to use in parsing
     * @param strictValidation If true, do not ignore invalid members
     * @return Query the corresponding Query object if parsing is successful
     * @throws MondrianException if parsing fails
     */
    public QueryPart parseStatement(
        String query,
        FunTable funTable,
        boolean strictValidation)
    {
        Parser parser = new Parser();
        boolean debug = false;

        if (funTable == null) {
            funTable = getSchema().getFunTable();
        }

        if (getLogger().isDebugEnabled()) {
            //debug = true;
            getLogger().debug(
                Util.nl
                + query);
        }

        try {
            return
                parser.parseInternal(
                    this, query, debug, funTable, strictValidation);
        } catch (Exception e) {
            throw MondrianResource.instance().FailedToParseQuery.ex(query, e);
        }
    }

    public Exp parseExpression(String expr) {
        boolean debug = false;
        if (getLogger().isDebugEnabled()) {
            //debug = true;
            getLogger().debug(
                Util.nl
                + expr);
        }
        try {
            Parser parser = new Parser();
            final FunTable funTable = getSchema().getFunTable();
            return parser.parseExpression(this, expr, debug, funTable);
        } catch (Throwable exception) {
            throw MondrianResource.instance().FailedToParseQuery.ex(
                expr,
                exception);
        }
    }
}

// End ConnectionBase.java
