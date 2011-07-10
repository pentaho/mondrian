/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/
package mondrian.olap;

import mondrian.parser.*;
import mondrian.resource.MondrianResource;

import mondrian.rolap.RolapConnection;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.server.Statement;
import mondrian.server.StatementImpl;
import mondrian.spi.ProfileHandler;
import org.apache.log4j.Logger;

import java.sql.SQLException;

/**
 * <code>ConnectionBase</code> implements some of the methods in
 * {@link Connection}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 */
public abstract class ConnectionBase implements Connection {

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
        final Statement statement = createDummyStatement();
        final Locus locus =
            new Locus(
                new Execution(statement, 0),
                "Parse/validate MDX statement",
                null);
        Locus.push(locus);
        try {
            return parseStatement(statement, query, null, false);
        } finally {
            Locus.pop(locus);
            statement.close();
        }
    }

    public Statement createDummyStatement() {
        return new StatementImpl() {
            public void close() {
            }

            public RolapConnection getMondrianConnection() {
                return (RolapConnection) ConnectionBase.this;
            }
        };
    }

    public Query parseQuery(String query) {
        return (Query) parseStatement(query);
    }

    public Query parseQuery(String query, boolean load) {
        Statement statement = createDummyStatement();
        try {
            return (Query) parseStatement(statement, query, null, false);
        } finally {
            statement.close();
        }
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
     * @param statement Evaluation context
     * @param query MDX query that requires special parsing
     * @param funTable Customized function table to use in parsing
     * @param strictValidation If true, do not ignore invalid members
     * @return Query the corresponding Query object if parsing is successful
     * @throws MondrianException if parsing fails
     */
    public QueryPart parseStatement(
        Statement statement,
        String query,
        FunTable funTable,
        boolean strictValidation)
    {
        MdxParserValidator parser = createParser();
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
                    statement, query, debug, funTable, strictValidation);
        } catch (Exception e) {
            throw MondrianResource.instance().FailedToParseQuery.ex(query, e);
        }
    }

    private MdxParserValidator createParser() {
        return true
            ? new JavaccParserValidatorImpl()
            : new MdxParserValidatorImpl();
    }

    public Exp parseExpression(String expr) {
        boolean debug = false;
        if (getLogger().isDebugEnabled()) {
            //debug = true;
            getLogger().debug(
                Util.nl
                + expr);
        }
        final Statement statement = createDummyStatement();
        try {
            MdxParserValidator parser = createParser();
            final FunTable funTable = getSchema().getFunTable();
            return parser.parseExpression(statement, expr, debug, funTable);
        } catch (Throwable exception) {
            throw MondrianResource.instance().FailedToParseQuery.ex(
                expr,
                exception);
        }
    }

}

// End ConnectionBase.java
