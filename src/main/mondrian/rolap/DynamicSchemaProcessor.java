/*
//$Id$
//This software is subject to the terms of the Common Public License
//Agreement, available at the following URL:
//http://www.opensource.org/licenses/cpl.html.
//Copyright (C) 2004-2005 TONBELLER AG
//All Rights Reserved.
//You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.net.URL;

/**
 * A dynamic schema processor is used to dynamically change
 * a Mondrian schema at runtime.
 */
public interface DynamicSchemaProcessor {

    /**
     * modify a Mondrian schema
     * @param schemaUrl - the catalog URL
     * @return the modified schema
     */
    public String processSchema(URL schemaUrl) throws Exception ;

} // DynamicSchemaProcessor
