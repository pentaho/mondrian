/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.parser;

import mondrian.olap.*;

import java.util.List;

/**
 * Parses and validates an MDX statement.
 *
 * <p>NOTE: API is subject to change. Current implementation is backwards
 * compatible with the old parser based on JavaCUP.
 *
 * @version $Id$
 * @author jhyde
 */
public interface MdxParserValidator {

    /**
      * Parses a string to create a {@link mondrian.olap.Query}.
      * Called only by {@link mondrian.olap.ConnectionBase#parseQuery}.
      */
    QueryPart parseInternal(
        Connection mdxConnection,
        String queryString,
        boolean debug,
        FunTable funTable,
        boolean strictValidation);

    Exp parseExpression(
        Connection connection,
        String queryString,
        boolean debug,
        FunTable funTable);

    interface QueryPartFactory {

        /**
         * Creates a {@link mondrian.olap.Query} object.
         * Override this function to make your kind of query.
         */
        Query makeQuery(
            Connection connection,
            Formula[] formulae,
            QueryAxis[] axes,
            String cube,
            Exp slicer,
            QueryPart[] cellProps,
            boolean strictValidation);

        /**
         * Creates a {@link mondrian.olap.DrillThrough} object.
         */
        DrillThrough makeDrillThrough(
            Query query,
            int maxRowCount,
            int firstRowOrdinal,
            List<Exp> returnList);

        /**
         * Creates an {@link mondrian.olap.Explain} object.
         */
        Explain makeExplain(
            QueryPart query);
    }
}

// End MdxParserValidator.java
