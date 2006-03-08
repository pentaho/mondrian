/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import mondrian.olap.Util;
import mondrian.rolap.DynamicSchemaProcessor;

import java.io.*;
import java.net.URL;

/**
 * Implementation of {@link DynamicSchemaProcessor} which reads the contents
 * of a {@link URL} then passes it through a filter.
 *
 * <p>This class is convenient for writing tests on schemas which are
 * modifications to the standard FoodMart schema. For example, the
 * {@link UdfTest} adds the declarations of two user-defined functions to
 * the end of the schema file.
 *
 * @author jhyde
 * @since Jun 28, 2005
 * @version $Id$
 */
public class DecoratingSchemaProcessor implements DynamicSchemaProcessor {
    /**
     * Returns the contents of a URL.
     */
    protected static String contentsOfUrl(URL schemaUrl) throws IOException {
        final InputStream is = schemaUrl.openStream();
        byte[] buf = new byte[2048];
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while (true) {
            int byteCount = is.read(buf);
            if (byteCount < 0) {
                break;
            }
            baos.write(buf, 0, byteCount);
        }
        return new String(baos.toByteArray());
    }

    public String processSchema(
            URL schemaUrl, Util.PropertyList properties) throws Exception {
        String schema = contentsOfUrl(schemaUrl);
        String filteredSchema = filterSchema(schema);
        return filteredSchema;
    }

    /**
     * Applies a filter to a schema string, and returns the result.
     *
     * <p>The default implementation returns the schema string unchanged.
     */
    protected String filterSchema(String s) {
        return s;
    }
}

// End DecoratingSchemaProcessor.java
