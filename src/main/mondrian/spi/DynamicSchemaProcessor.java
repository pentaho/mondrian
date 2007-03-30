/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.Util;

import java.net.URL;

/**
 * A dynamic schema processor is used to dynamically change
 * a Mondrian schema at runtime.
 */
public interface DynamicSchemaProcessor {

    /**
     * Modifies a Mondrian schema.
     *
     * @param schemaUrl the catalog URL
     * @param connectInfo Connection properties
     * @return the modified schema
     */
    public String processSchema(
            URL schemaUrl,
            Util.PropertyList connectInfo) throws Exception;
}

// End DynamicSchemaProcessor.java

