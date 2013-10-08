/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.Util;
import mondrian.olap.fun.vba.Vba;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder for JSON documents (represented as {@link List}, {@link Map},
 * {@link String}, {@link Boolean}, {@link Long}).
 */
public class JsonBuilder {
    /** Creates a JSON object (represented by a {@link Map}). */
    public Map<String, Object> map() {
        // Use LinkedHashMap to preserve order.
        return new LinkedHashMap<String, Object>();
    }

    /** Creates a JSON object (represented by a {@link List}). */
    public List<Object> list() {
        return new ArrayList<Object>();
    }

    /** Adds a key/value pair to a JSON object. */
    public JsonBuilder put(Map<String, Object> map, String name, Object value) {
        map.put(name, value);
        return this;
    }

    /** Adds a key/value pair to a JSON object if the value is not null. */
    public JsonBuilder putIf(
        Map<String, Object> map, String name, Object value)
    {
        if (value != null) {
            map.put(name, value);
        }
        return this;
    }

    /**
     * Serializes an object consisting of maps, lists and atoms into a JSON
     * string.
     *
     * <p>We should use a JSON library such as Jackson when Mondrian needs
     * one elsewhere.</p>
     */
    public String toJsonString(Object o) {
        StringBuilder buf = new StringBuilder();
        append(buf, 0, o);
        return buf.toString();
    }

    /** Appends a JSON object to a string builder. */
    private void append(StringBuilder buf, int indent, Object o) {
        if (o instanceof Map) {
            //noinspection unchecked
            appendMap(buf, indent, (Map) o);
        } else if (o instanceof List) {
            //noinspection unchecked
            appendList(buf, indent, (List) o);
        } else if (o instanceof String) {
            Util.singleQuoteString((String) o, buf);
        } else {
            assert o instanceof Number || o instanceof Boolean;
            buf.append(o);
        }
    }

    private void appendMap(
        StringBuilder buf, int indent, Map<String, Object> map)
    {
        if (map.isEmpty()) {
            buf.append("{}");
            return;
        }
        buf.append("{");
        newline(buf, indent + 1);
        int n = 0;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (n++ > 0) {
                buf.append(",");
                newline(buf, indent + 1);
            }
            buf.append(entry.getKey());
            buf.append(": ");
            append(buf, indent + 1, entry.getValue());
        }
        newline(buf, indent);
        buf.append("}");
    }

    private void newline(StringBuilder buf, int indent) {
        buf.append('\n')
            .append(Vba.space(indent * 2));
    }

    private void appendList(
        StringBuilder buf, int indent, List<Object> list)
    {
        if (list.isEmpty()) {
            buf.append("[]");
            return;
        }
        buf.append("[");
        newline(buf, indent + 1);
        int n = 0;
        for (Object o  : list) {
            if (n++ > 0) {
                buf.append(",");
                newline(buf, indent + 1);
            }
            append(buf, indent + 1, o);
        }
        newline(buf, indent);
        buf.append("]");
    }
}

// End JsonBuilder.java
