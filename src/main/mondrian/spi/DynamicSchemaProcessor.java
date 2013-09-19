/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2007 Pentaho
// All Rights Reserved.
*/

package mondrian.spi;

import mondrian.olap.Util;

/**
 * A dynamic schema processor is used to dynamically change
 * a Mondrian schema at runtime.
 *
 * <p>Mondrian loads a DynamicSchemaProcessor when it sees the
 * {@link mondrian.rolap.RolapConnectionProperties#DynamicSchemaProcessor}
 * keyword in a connect string. The value of that property must be a class
 * which implements this interface. Rather than loading the schema directly,
 * Mondrian instantiates the class and calls the
 * {@link #processSchema(String, mondrian.olap.Util.PropertyList)} method
 * with the catalog URL and connection properties specified in the connect
 * string.
 *
 * <p>By default, mondrian uses Apache VFS (virtual file system) to resolve
 * catalog URLs. We recommend that implementations of DynamicSchemaProcessor
 * do the same.
 *
 * <p>If you are writing an implementation of this class, we recommend that
 * you use {@link mondrian.spi.impl.FilterDynamicSchemaProcessor} as a
 * base class.
 *
 * @author hhaas
 */
public interface DynamicSchemaProcessor {

    /**
     * Modifies a Mondrian schema.
     *
     * <p>An implementation should generally interpret the URL string as
     * an Apache VFS (virtual file system) URL.
     *
     * @param schemaUrl the URL of the catalog
     * @param connectInfo Connection properties
     * @return the modified schema
     * @throws Exception if an error occurs
     */
    public String processSchema(
        String schemaUrl,
        Util.PropertyList connectInfo) throws Exception;
}

// End DynamicSchemaProcessor.java

