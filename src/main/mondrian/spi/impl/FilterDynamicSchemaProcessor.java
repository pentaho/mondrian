/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import mondrian.spi.DynamicSchemaProcessor;
import mondrian.olap.Util;
import org.apache.commons.vfs.FileSystemManager;
import org.apache.commons.vfs.VFS;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileContent;

import java.io.File;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Implementation of {@link DynamicSchemaProcessor} which allows a derived class
 * to easily process a schema file.
 *
 * <p>Mondrian's default mechanism for loading schema files, if no
 * DynamicSchemaProcessor is specified, is to use Apache VFS (virtual file
 * system) to resolve the URL to a stream, and to read the contents of the
 * stream into a string.
 *
 * <p>FilterDynamicSchemaProcessor implements exactly the
 * same mechanism, but makes it easy for a derived class to override the
 * mechanism. For example:<ul>
 *
 * <li>To redirect to a different URL, override the
 * {@link #processSchema(String, mondrian.olap.Util.PropertyList)} method.
 *
 * <li>To process the contents of the URL, override the
 * {@link #filter(String, mondrian.olap.Util.PropertyList, java.io.InputStream)}
 * method.
 *
 * </ul>
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 30, 2007
 */
public class FilterDynamicSchemaProcessor implements DynamicSchemaProcessor {

    /**
     * {@inheritDoc}
     *
     * <p>FilterDynamicSchemaProcessor's implementation of this method reads
     * from the URL supplied (that is, it does not perform URL translation)
     * and passes it through the {@link #filter} method.
     */
    public String processSchema(
        String schemaUrl,
        Util.PropertyList connectInfo) throws Exception
    {
        FileSystemManager fsManager = VFS.getManager();
        File userDir = new File("").getAbsoluteFile();
        FileObject file = fsManager.resolveFile(userDir, schemaUrl);
        FileContent fileContent = file.getContent();
        return filter(schemaUrl, connectInfo, fileContent.getInputStream());
    }

    /**
     * Reads the contents of a schema as a stream and returns the result as
     * a string.
     *
     * <p>The default implementation returns the contents of the schema
     * unchanged.
     *
     * @param schemaUrl the URL of the catalog
     * @param connectInfo Connection properties
     * @param stream Schema contents represented as a stream
     * @return the modified schema
     * @throws Exception if an error occurs
     */
    protected String filter(
        String schemaUrl,
        Util.PropertyList connectInfo,
        InputStream stream)
        throws Exception
    {
        BufferedReader in =
            new BufferedReader(
                new InputStreamReader(stream));
        try {
            StringBuilder builder = new StringBuilder();
            char[] buf = new char[2048];
            int readCount;
            while ((readCount = in.read(buf, 0, buf.length)) >= 0) {
                builder.append(buf, 0, readCount);
            }
            return builder.toString();
        } finally {
            in.close();
        }
    }
}

// End FilterDynamicSchemaProcessor.java
