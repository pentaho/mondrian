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
 * Default implementation of {@link mondrian.parser.MdxParserValidator}.
 *
 * @author jhyde
 */
public class MdxParserValidatorImpl implements MdxParserValidator {
    /**
     * Creates a MdxParserValidatorImpl.
     */
    public MdxParserValidatorImpl() {
    }

    public QueryPart parseInternal(
        Statement statement,
        String queryString,
        boolean debug,
        FunTable funTable,
        boolean strictValidation)
    {
        return new Parser().parseInternal(
            new Parser.FactoryImpl(),
            statement, queryString, debug, funTable, strictValidation);
    }

    public Exp parseExpression(
        Statement statement,
        String queryString,
        boolean debug,
        FunTable funTable)
    {
        return new Parser().parseExpression(
            new Parser.FactoryImpl(),
            statement, queryString, debug, funTable);
    }
}

// End MdxParserValidatorImpl.java
