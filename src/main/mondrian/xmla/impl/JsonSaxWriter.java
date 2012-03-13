/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2010 Pentaho
// All Rights Reserved.
*/
package mondrian.xmla.impl;

import mondrian.olap.Util;
import mondrian.util.ArrayStack;
import mondrian.xmla.SaxWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Implementation of <code>SaxWriter</code> which, perversely, generates a
 * JSON (JavaScript Object Notation) document.
 *
 * @author jhyde
 */
class JsonSaxWriter implements SaxWriter {
    private final StringBuilder buf = new StringBuilder();
    private int indent;
    private String[] indentStrings = INITIAL_INDENT_STRINGS;
    private String indentString = indentStrings[0];
    private final ArrayStack<Frame> stack = new ArrayStack<Frame>();
    private OutputStream outputStream;

    private static final String[] INITIAL_INDENT_STRINGS = {
        "",
        "  ",
        "    ",
        "      ",
        "        ",
        "          ",
        "            ",
        "              ",
        "                ",
        "                  ",
    };

    /**
     * Creates a JsonSaxWriter.
     *
     * @param outputStream Output stream
     */
    public JsonSaxWriter(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public void startDocument() {
        stack.push(new Frame(null));
    }

    public void endDocument() {
        stack.pop();
        flush();
    }

    public void startSequence(String name, String subName) {
        comma();
        buf.append(indentString);
        if (name == null) {
            name = subName;
        }
        if (stack.peek().name != null) {
            assert name.equals(stack.peek().name)
                : "In sequence [" + stack.peek() + "], element name ["
                  + name + "]";
            buf.append("[");
        } else {
            Util.quoteForMdx(buf, name);
            buf.append(": [");
        }

        assert subName != null;
        stack.push(new Frame(subName));
        indent();
    }

    public void endSequence() {
        assert stack.peek() != null : "not in sequence";
        stack.pop();
        outdent();

        buf.append("\n");
        buf.append(indentString);
        buf.append("]");
    }

    public void startElement(String name) {
        comma();
        buf.append(indentString);
        if (stack.peek().name != null) {
            assert name.equals(stack.peek().name)
                : "In sequence [" + stack.peek() + "], element name ["
                  + name + "]";
            buf.append("{");
        } else {
            Util.quoteForMdx(buf, name);
            buf.append(": {");
        }

        stack.push(new Frame(null));
        indent();
    }

    public void startElement(String name, Object... attrs) {
        startElement(name);
        for (int i = 0; i < attrs.length;) {
            if (i > 0) {
                buf.append(",\n");
            } else {
                buf.append("\n");
            }
            String attr = (String) attrs[i++];
            buf.append(indentString);
            Util.quoteForMdx(buf, attr);
            buf.append(": ");
            Object value = attrs[i++];
            value(value);
        }
        stack.peek().ordinal = attrs.length / 2;
    }

    public void endElement() {
        Frame prev = stack.pop();
        assert prev.name == null
            : "Ended an element, but in sequence " + prev.name;
        buf.append("\n");
        outdent();
        buf.append(indentString);
        buf.append("}");
    }

    public void element(String name, Object... attrs) {
        startElement(name, attrs);
        endElement();
    }

    public void characters(String data) {
        throw new UnsupportedOperationException();
    }

    public void textElement(String name, Object data) {
        comma();
        buf.append(indentString);
        Util.quoteForMdx(buf, name);
        buf.append(": ");
        value(data);
    }

    public void completeBeforeElement(String tagName) {
        throw new UnsupportedOperationException();
    }

    public void verbatim(String text) {
        throw new UnsupportedOperationException();
    }

    public void flush() {
        try {
            outputStream.write(buf.toString().substring(1).getBytes());
        } catch (IOException e) {
            throw Util.newError(e, "While encoding JSON response");
        }
    }

    // helper methods

    private void indent() {
        ++indent;
        if (indent >= indentStrings.length) {
            final int newLength = indentStrings.length * 2 + 1;
            final int INDENT = 2;
            assert indentStrings[1].length() == INDENT;
            char[] chars = new char[newLength * INDENT];
            Arrays.fill(chars, ' ');
            String s = new String(chars);
            indentStrings = new String[newLength];
            for (int i = 0; i < newLength; ++i) {
                indentStrings[i] = s.substring(0, i * INDENT);
            }
        }
        indentString = indentStrings[indent];
    }

    private void outdent() {
        indentString = indentStrings[--indent];
    }

    /**
     * Writes a value with appropriate quoting for a JavaScript constant
     * of that type.
     *
     * <p>Examples: {@code "a \"quoted\" string"} (string),
     * {@code 12} (int), {@code 12.345} (float), {@code null} (null value).
     *
     * @param value Value
     */
    private void value(Object value) {
        if (value instanceof String) {
            String s = (String) value;
            Util.quoteForMdx(buf, s);
        } else {
            buf.append(value);
        }
    }

    private void comma() {
        if (stack.peek().ordinal++ > 0) {
            buf.append(",\n");
        } else {
            buf.append("\n");
        }
    }

    private static class Frame {
        final String name;
        int ordinal;

        Frame(String name) {
            this.name = name;
        }
    }
}

// End JsonSaxWriter.java
