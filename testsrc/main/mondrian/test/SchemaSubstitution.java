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
}
// End SchemaSubstitution.java