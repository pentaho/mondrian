/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import mondrian.olap.Util;
import mondrian.resource.MondrianResource;

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
 * @version $Id$
 */
public class UtilCompatibleJdk16 extends UtilCompatibleJdk15 {
    private final static Logger LOGGER =
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
            if (!stmt.isClosed()) {
                stmt.cancel();
            }
        } catch (SQLException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    MondrianResource.instance()
                        .ExecutionStatementCleanupException
                            .ex(e.getMessage(), e),
                    e);
            }
        }
        try {
            if (!stmt.isClosed()) {
                stmt.close();
            }
        } catch (SQLException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
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
        return Arrays.binarySearch(ts, start, end, t);
    }
}

// End UtilCompatibleJdk16.java
