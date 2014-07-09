/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2013 Pentaho
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

    public static Util.Function1<String, String> insertColumnDef(
        final String tableName,
        final String columnDef)
    {
        return new Util.Function1<String, String>() {
            public String apply(String schema) {
                String prefix = "";
                String suffix = "";
                int h = schema.indexOf("<Table name='" + tableName + "'");
                if (h < 0) {
                    throw new RuntimeException(
                        "table '" + tableName + "' not found");
                }
                int end = schema.indexOf("</Table>", h);

                int existingDefs = schema.indexOf("<ColumnDefs>", h);
                if (existingDefs == -1) {
                    // There wasn't a ColumnDefs element yet.
                    // We create it.
                    prefix = "<ColumnDefs>\n";
                    suffix = "</ColumnDefs>\n";
                } else {
                    if (existingDefs < end) {
                        // We change the insertion markers so that they don't
                        // the existing ColumnDefs.
                        end = schema.indexOf("</ColumnDefs>", existingDefs);
                    }
                }

                return
                    schema.substring(0, end)
                        .concat(prefix)
                        .concat(columnDef)
                        .concat(suffix)
                        .concat(schema.substring(end));
            }
        };
    }

    public static Util.Function1<String, String> insertDimension(
        final String cubeName, final String dimDefs)
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
                       + dimDefs
                       + schema.substring(i);
            }
        };
    }

    public static Util.Function1<String, String> insertDimensionLinks(
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

    public static Util.Function1<String, String> insertHierarchy(
        final String cubeName,
        final String dimensionName,
        final String hierarchyDefs)
    {
        return new Util.Function1<String, String>() {
            public String apply(String schema) {
                int h = schema.indexOf("<Cube name='" + cubeName + "'");
                if (h < 0) {
                    throw new RuntimeException(
                        "cube '" + cubeName + "' not found");
                }
                int hEnd = schema.indexOf("</Cube>", h);

                int i =
                    schema.indexOf(
                        "<Dimension name='" + dimensionName + "'", h);
                if (i < 0 || i >= hEnd) {
                    throw new RuntimeException(
                        "dimension '" + dimensionName + "' not found");
                }
                int iEnd = schema.indexOf("</Dimension>", i);

                // Insert before first hierarchy. TOOD: handle if there are no
                // hierarchies yet.
                int j = schema.indexOf("<Hierarchy ", i);
                if (j < 0 || j >= iEnd) {
                    throw new RuntimeException("no hierarchy");
                }

                return schema.substring(0, j)
                    + hierarchyDefs
                    + schema.substring(j);
            }
        };
    }

    public static Util.Function1<String, String> insertCalculatedMembers(
        final String cubeName, final String memberDefs)
    {
        return new Util.Function1<String, String>() {
            public String apply(String schema) {
                int h = schema.indexOf("<Cube name='" + cubeName + "'");
                int i = schema.indexOf("<CalculatedMembers>", h);
                if (i >= 0) {
                    i += "<CalculatedMembers>".length();
                    schema = schema.substring(0, i)
                             + memberDefs
                             + schema.substring(i);
                } else {
                    int end = schema.indexOf("</Cube>", h);
                    i = schema.indexOf("<CalculatedMember", h);
                    if (i < 0 || i > end) {
                        i = end;
                    }
                    schema = schema.substring(0, i)
                             + "<CalculatedMembers>"
                             + memberDefs
                             + "</CalculatedMembers>"
                             + schema.substring(i);
                }
                return schema;
            }
        };
    }

    public static Util.Function1<String, String> insertRole(
        final String roleDef)
    {
        return new Util.Function1<String, String>() {
            public String apply(String schema) {
                // <Role> will be last element of the schema
                int h = schema.indexOf("</Schema>");
                if (h >= 0) {
                    schema = schema.substring(0, h)
                        + roleDef
                        + schema.substring(h);
                }
                return schema;
            }
        };
    }

    public static Util.Function1<String, String> insertSharedDimension(
        final String sharedDimension)
    {
        return new Util.Function1<String, String>() {
            public String apply(String schema) {
                int i = schema.indexOf("</PhysicalSchema>");
                return schema.substring(0, i + 17)
                       + sharedDimension
                       + schema.substring(i + 17);
            }
        };
    }

    public static Util.Function1<String, String> replacePhysSchema(
        final String physSchema)
    {
        return  new Util.Function1<String, String>() {
            public String apply(String schema) {
                final int start = schema.indexOf("<PhysicalSchema");
                final int end = schema.indexOf(
                    "</PhysicalSchema>") + "</PhysicalSchema>".length();
                return schema.substring(0, start)
                    + physSchema
                    + schema.substring(end);
            }
        };
    }

    public static Util.Function1<String, String> replace(
        final String search, final String substitution)
    {
        return new Util.Function1<String, String>() {
            public String apply(String schema) {
                final int start = schema.indexOf(search);
                final int end = start + search.length();
                return schema.substring(0, start)
                    + substitution
                    + schema.substring(end);
            }
        };
    }

    public static Util.Function1<String, String> remove(
        final String xml)
    {
        return new Util.Function1<String, String>() {
            public String apply(String schema) {
                final int start = schema.indexOf(xml);
                return schema.substring(0, start)
                    + schema.substring(start + xml.length());
            }
        };
    }
}
// End SchemaSubstitution.java
