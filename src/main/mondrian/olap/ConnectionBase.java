/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

import org.apache.log4j.Logger;

/**
 * <code>ConnectionBase</code> implements some of the methods in
 * {@link Connection}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public abstract class ConnectionBase implements Connection {

    protected ConnectionBase() {
    }

    protected abstract Logger getLogger();

    public String getFullConnectString() {
        String s = getConnectString();
        String catalogName = getCatalogName();
        if (catalogName != null) {
            int len = s.length() + catalogName.length() + 32;
            StringBuffer buf = new StringBuffer(len);
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

    public Query parseQuery(String s) {
        try {
            boolean debug = false;
            Parser parser = new Parser();
            Query q = parser.parseInternal(this, s, debug);
            return q;
        } catch (Throwable e) {
            throw MondrianResource.instance().newFailedToParseQuery(s, e);
        }
    }

    public Exp parseExpression(String s) {
        Util.assertTrue(
                s.startsWith("'") && s.endsWith("'"),
                "only string literals are supported right now");

        String s2 = s.substring(1, s.length() - 1);
        return Literal.createString(s2);
    }
}

// End ConnectionBase.java
