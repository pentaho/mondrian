/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2012 Pentaho
// All Rights Reserved.
*/
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
        } catch (TokenMgrError e) {
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
        } catch (TokenMgrError e) {
            throw convertException(queryString, e);
        }
    }

    /**
     * Converts the exception so that it looks like the exception produced by
     * JavaCUP. (Not that that format is ideal, but it minimizes test output
     * changes during the transition from JavaCUP to JavaCC.)
     *
     * @param queryString MDX query string
     * @param throwable JavaCC parse exception or {@link TokenMgrError}
     * @return Wrapped exception
     */
    private RuntimeException convertException(
        String queryString,
        Throwable throwable)
    {
        Throwable e = throwable;
        if (throwable instanceof ParseException) {
            ParseException pe = (ParseException) throwable;
            if (pe.getMessage().startsWith("Encountered ")) {
                e = new MondrianException(
                    "Syntax error at line "
                    + pe.currentToken.next.beginLine
                    + ", column "
                    + pe.currentToken.next.beginColumn
                    + ", token '"
                    + pe.currentToken.next.image
                    + "'");
            }
        } else {
            // Typical message:
            //   Lexical error at line 1, column 1.  Encountered: "`" (96),
            //   after : ""
            e = new MondrianException(e.getMessage());
        }
        return Util.newError(e, "While parsing " + queryString);
    }
}

// End JavaccParserValidatorImpl.java
