/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.Util;

import java.util.Map;

public class SchemaSubstitution {
    public static Util.Function1<String, String> insertCube(
        final String cubeDef)
    {
        return new Util.Function1<String, String>() {
            public String apply(String schema) {
                int i =
                    schema.indexOf(
                        "<Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">");
                if (i < 0) {
                    i = schema.indexOf(
                        "<Cube name='Sales' defaultMeasure='Unit Sales'>");
                }
                return schema.substring(0, i)
                       + cubeDef
                       + schema.substring(i);
            }
        };
    }

    public static Util.Function1<String, String> insertPhysTable(
        final String tableDef)
    {
        return new Util.Function1<String, String>() {
            public String apply(String schema) {
                int i = schema.indexOf("</PhysicalSchema>");
                return schema.substring(0, i)
                    + tableDef
                    + schema.substring(i);
            }
        };
    }

    public static Util.Function1<String, String> insertDimension(
        final String cubeName, final String dimensionDefs)
    {
        return new Util.Function1<String, String>() {
            public String apply(String schema) {
                int h = schema.indexOf("<Cube name='" + cubeName + "'");
                if (h < 0) {
                    throw new RuntimeException(
                        "cube '" + cubeName + "' not found");
                }

                int i = schema.indexOf("<Dimension ", h);
                return schema.substring(0, i)
                       + dimensionDefs
                       + schema.substring(i);
            }
        };
    }

    public static Util.Function1<String, String> insertDimensionLink(
        final String cubeName, final Map<String, String> dimLinks)
    {
        return new Util.Function1<String, String>() {
            public String apply(String schema) {
                int h = schema.indexOf("<Cube name='" + cubeName + "'");
                if (h < 0) {
                    throw new RuntimeException(
                        "cube '" + cubeName + "' not found");
                }
                int end = schema.indexOf("</Cube>", h);
                for (Map.Entry<String, String> entry : dimLinks.entrySet()) {
                    int i =
                        schema.indexOf(
                            "<MeasureGroup name='" + entry.getKey() + "' ",
                            h);
                    if (i < 0 || i > end) {
                        continue;
                    }
                    i = schema.indexOf("</DimensionLinks>", i);
                    if (i < 0 || i > end) {
                        continue;
                    }
                    schema = schema.substring(0, i)
                        + entry.getValue()
                        + schema.substring(i);
                }

                return schema;
            }
        };
    }

    public static Util.Function1<String, String> ignoreMissingLink() {
        return new Util.Function1<String, String>() {
            public String apply(String schema) {
                int h = schema.indexOf("<Schema ");
                h = schema.indexOf(">", h);
                schema =
                    schema.substring(0, h)
                    + " missingLink='ignore'" + schema.substring(h);
                return schema;
            }
        };
    }
}
// End SchemaSubstitution.java