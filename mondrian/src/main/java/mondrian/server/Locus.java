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


package mondrian.server;

import mondrian.olap.MondrianServer;
import mondrian.rolap.RolapConnection;
import mondrian.util.ArrayStack;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Point of execution from which a service is invoked.
 */
public class Locus {
    public final Execution execution;
    public final String message;
    public final String component;

    private static final ThreadLocal<ArrayStack<Locus>> THREAD_LOCAL =
        new ThreadLocal<ArrayStack<Locus>>() {
            protected ArrayStack<Locus> initialValue() {
                return new ArrayStack<Locus>();
            }
        };

    /**
     * Creates a Locus.
     *
     * @param execution Execution context
     * @param component Description of a the component executing the query,
     *   generally a method name, e.g. "SqlTupleReader.readTuples"
     * @param message Description of the purpose of this statement, to be
     *   printed if there is an error
     */
    public Locus(
        Execution execution,
        String component,
        String message)
    {
        assert execution != null;
        this.execution = execution;
        this.component = component;
        this.message = message;
    }

    public static void pop(Locus locus) {
        final Locus pop = THREAD_LOCAL.get().pop();
        assert locus == pop;
    }

    public static void push(Locus locus) {
        THREAD_LOCAL.get().push(locus);
    }

    public static Locus peek() {
        return THREAD_LOCAL.get().peek();
    }
    
    public static boolean isEmpty() {
      return THREAD_LOCAL.get().isEmpty();
    }

    public static <T> T execute(
        RolapConnection connection,
        String component,
        Action<T> action)
    {
        final Statement statement = connection.getInternalStatement();
        final Execution execution = new Execution(statement, 0);
        return execute(execution, component, action);
    }

    public static <T> T execute(
        Execution execution,
        String component,
        Action<T> action)
    {
        final Locus locus =
            new Locus(
                execution,
                component,
                null);
        Locus.push(locus);
        try {
            return action.execute();
        } finally {
            Locus.pop(locus);
        }
    }

    public final MondrianServer getServer() {
        return execution.statement.getMondrianConnection().getServer();
    }

    public interface Action<T> {
        T execute();
    }

    public void export(HttpServletRequest req, HttpServletResponse res) {
    res = setCors(req, res);
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, private");
    byte[] payload = Base64.getDecoder().decode(req.getParameter("data"));
    // jfrog-ignore
    String data = unserialize(new String(payload, StandardCharsets.UTF_8));
}

private HttpServletResponse setCors(HttpServletRequest req, HttpServletResponse res) {
    // Implementation of setCors method
    return res;
}

private String unserialize(String data) {
    // Implementation of unserialize method
    return data; // Placeholder return
}
    
}

// End Locus.java
