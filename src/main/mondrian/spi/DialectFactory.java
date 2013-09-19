/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.spi;

import java.sql.Connection;
import javax.sql.DataSource;

/**
 * Factory that creates {@link Dialect} objects.
 *
 * <p>If you create a class that implements {@link Dialect}, you may optionally
 * provide a factory by creating a constant field in that class. For
 * example:</p>
 *
 * <pre>
 * public class MyDialect implements Dialect {
 *     public static final DialectFactory FACTORY =
 *         new JdbcDialectFactory(MyDialect.class, null);
 *
 *     public MyDialect(Connection connection) {
 *         ...
 *     }
 *
 *     ...
 * }</pre>
 *
 * <p>(The field must be public, static, final, named "FACTORY", of type
 * {@link mondrian.spi.DialectFactory} or a subclass, and its value must not be
 * null.)</p>
 *
 * <p>Explicitly providing a factory gives you more control about how dialects
 * are produced.</p>
 *
 * <p>If you do not provide such a field, Mondrian requires that the dialect has
 * a public constructor that takes a {@link java.sql.Connection} as a parameter,
 * and automatically creates a factory that calls the class's public
 * constructor.</p>
 *
 * <p>However, an explicit DialectFactory is superior:<ol>
 * <li>When a dialect cannot handle a given connection, a dialect factory can
 *     return {@code null}, whereas a dialect's constructor can only throw an
 *     exception.
 * <li>A dialect factory can maintain a pool or cache of dialect objects.
 * </ol>
 *
 * <p>If your dialect is a subclass of {@link mondrian.spi.impl.JdbcDialectImpl}
 * you may wish to use a dialect factory that is a subclass of
 * {@link mondrian.spi.impl.JdbcDialectFactory}.
 *
 * @author jhyde
 * @since Jan 13, 2009
 */
public interface DialectFactory {
    /**
     * Creates a Dialect.
     *
     * <p>If the dialect cannot handle this connection, returns null.
     *
     * @param dataSource JDBC data source
     * @param connection JDBC connection
     *
     * @return dialect for this connection, or null if this factory's dialect
     * is not appropriate for the connection
     *
     * @throws RuntimeException if underlying systems give an error
     */
    Dialect createDialect(
        DataSource dataSource,
        Connection connection);
}

// End DialectFactory.java
