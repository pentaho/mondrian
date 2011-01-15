/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.parser;

import mondrian.olap.*;

/**
 * Default implementation of {@link mondrian.parser.MdxParserValidator}.
 *
 * @version $Id$
 * @author jhyde
 */
public class MdxParserValidatorImpl implements MdxParserValidator {
    /**
     * Creates a MdxParserValidatorImpl.
     */
    public MdxParserValidatorImpl() {
    }

    public QueryPart parseInternal(
        Connection connection,
        String queryString,
        boolean debug,
        FunTable funTable,
        boolean strictValidation)
    {
        return new Parser().parseInternal(
            new Parser.FactoryImpl(),
            connection, queryString, debug, funTable, strictValidation);
    }

    public Exp parseExpression(
        Connection connection,
        String queryString,
        boolean debug,
        FunTable funTable)
    {
        return new Parser().parseExpression(
            new Parser.FactoryImpl(),
            connection, queryString, debug, funTable);
    }
}

// End MdxParserValidatorImpl.java
