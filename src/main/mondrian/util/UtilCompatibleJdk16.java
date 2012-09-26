/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.Util;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapUtil;

import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import javax.script.*;

// Only in Java6 and above

/**
 * Implementation of {@link mondrian.util.UtilCompatible} that runs in
 * JDK 1.6.
 *
 * <p>Prior to JDK 1.6, this class should never be loaded. Applications should
 * instantiate this class via {@link Class#forName(String)} or better, use
 * methods in {@link mondrian.olap.Util}, and not instantiate it at all.
 *
 * @author jhyde
 */
public class UtilCompatibleJdk16 extends UtilCompatibleJdk15 {
    private static final Logger LOGGER =
        Logger.getLogger(Util.class);

    public <T> T compileScript(
        Class<T> iface,
        String script,
        String engineName)
    {
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName(engineName);
        try {
            engine.eval(script);
            Invocable inv = (Invocable) engine;
            return inv.getInterface(iface);
        } catch (ScriptException e) {
            throw Util.newError(
                e,
                "Error while compiling script to implement " + iface + " SPI");
        }
    }

    @Override
    public void cancelAndCloseStatement(Statement stmt) {
        try {
            // A call to statement.isClosed() would be great here, but in
            // reality, some drivers will block on this check and the
            // cancellation will never happen.  This is due to the
            // non-thread-safe nature of JDBC and driver implementations. If a
            // thread is currently using the statement, calls to isClosed() are
            // synchronized internally and won't return until the query
            // completes.
            stmt.cancel();
        } catch (SQLException e) {
            // We crush this one. A lot of drivers will complain if cancel() is
            // called on a closed statement, but a call to isClosed() isn't
            // thread safe and might block. See above.
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    MondrianResource.instance()
                        .ExecutionStatementCleanupException
                            .ex(e.getMessage(), e),
                    e);
            }
        }
        try {
            // We used to call Statement.isClosed, but DBCP gave error:
            //   java.lang.IllegalAccessError:
            //   org.apache.commons.dbcp.DelegatingStatement.isClosed()Z
            // JDBC says it is OK to call close on a closed statement, so
            // why check?
            stmt.close();
        } catch (SQLException e) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    MondrianResource.instance()
                        .ExecutionStatementCleanupException
                            .ex(e.getMessage(), e),
                    e);
            }
        }
    }

    @Override
    public <T> Set<T> newIdentityHashSet() {
        return Collections.newSetFromMap(
            new IdentityHashMap<T, Boolean>());
    }

    public <T extends Comparable<T>> int binarySearch(
        T[] ts, int start, int end, T t)
    {
        return Arrays.binarySearch(
            ts, start, end, t,
            RolapUtil.ROLAP_COMPARATOR);
    }
}

// End UtilCompatibleJdk16.java
