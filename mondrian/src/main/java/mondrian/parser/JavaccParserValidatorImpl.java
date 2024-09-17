/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package mondrian.parser;

import mondrian.olap.*;
import mondrian.server.Statement;

/**
 * Default implementation of {@link MdxParserValidator}, using the
 * <a href="http://java.net/projects/javacc/">JavaCC</a> parser generator.
 *
 * @author jhyde
 */
public class JavaccParserValidatorImpl implements MdxParserValidator {
    private final QueryPartFactory factory;

    /**
     * Creates a JavaccParserValidatorImpl.
     */
    public JavaccParserValidatorImpl() {
        this(new Parser.FactoryImpl());
    }

    /**
     * Creates a JavaccParserValidatorImpl with an explicit factory for parse
     * tree nodes.
     *
     * @param factory Factory for parse tree nodes
     */
    public JavaccParserValidatorImpl(QueryPartFactory factory) {
        this.factory = factory;
    }

    public QueryPart parseInternal(
        Statement statement,
        String queryString,
        boolean debug,
        FunTable funTable,
        boolean strictValidation)
    {
        final MdxParserImpl mdxParser =
            new MdxParserImpl(
                factory,
                statement,
                queryString,
                debug,
                funTable,
                strictValidation);
        try {
            return mdxParser.statementEof();
        } catch (ParseException e) {
            throw convertException(queryString, e);
        }
    }

    public Exp parseExpression(
        Statement statement,
        String queryString,
        boolean debug,
        FunTable funTable)
    {
        final MdxParserImpl mdxParser =
            new MdxParserImpl(
                factory,
                statement,
                queryString,
                debug,
                funTable,
                false);
        try {
            return mdxParser.expressionEof();
        } catch (ParseException e) {
            throw convertException(queryString, e);
        }
    }

    /**
     * Converts the exception so that it looks like the exception produced by
     * JavaCUP. (Not that that format is ideal, but it minimizes test output
     * changes during the transition from JavaCUP to JavaCC.)
     *
     * @param queryString MDX query string
     * @param pe JavaCC parse exception
     * @return Wrapped exception
     */
    private RuntimeException convertException(
        String queryString,
        ParseException pe)
    {
        Exception e;
        if (pe.getMessage().startsWith("Encountered ")) {
            e = new MondrianException(
                "Syntax error at line "
                + pe.currentToken.next.beginLine
                + ", column "
                + pe.currentToken.next.beginColumn
                + ", token '"
                + pe.currentToken.next.image
                + "'");
        } else {
            e = pe;
        }
        return Util.newError(e, "While parsing " + queryString);
    }
}

// End JavaccParserValidatorImpl.java
