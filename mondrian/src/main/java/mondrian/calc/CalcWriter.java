/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.calc;

import org.apache.commons.collections.map.CompositeMap;

import java.io.PrintWriter;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Visitor which serializes an expression to text.
 *
 * @author jhyde
 * @since Dec 23, 2005
 */
public class CalcWriter {
    private static final int INDENT = 4;
    private static String BIG_STRING = "                ";

    private final PrintWriter writer;
    private final boolean profiling;
    private int linePrefixLength;
    private final Map<Calc, Map<String, Object>> parentArgMap =
        new IdentityHashMap<Calc, Map<String, Object>>();

    public CalcWriter(PrintWriter writer, boolean profiling) {
        this.writer = writer;
        this.profiling = profiling;
    }

    public PrintWriter getWriter() {
        return writer;
    }

    public void visitChild(int ordinal, Calc calc) {
        indent();
        calc.accept(this);
        outdent();
    }

    public void visitCalc(
        Calc calc,
        String name,
        Map<String, Object> arguments,
        Calc[] childCalcs)
    {
        writer.print(getLinePrefix());
        writer.print(name);
        final Map<String, Object> parentArgs = parentArgMap.get(calc);
        if (parentArgs != null && !parentArgs.isEmpty()) {
            //noinspection unchecked
            arguments = new CompositeMap(arguments, parentArgs);
        }
        if (!arguments.isEmpty()) {
            writer.print("(");
            int k = 0;
            for (Map.Entry<String, Object> entry : arguments.entrySet()) {
                if (k++ > 0) {
                    writer.print(", ");
                }
                writer.print(entry.getKey());
                writer.print("=");
                writer.print(entry.getValue());
            }
            writer.print(")");
        }
        writer.println();
        int k = 0;
        for (Calc childCalc : childCalcs) {
            visitChild(k++, childCalc);
        }
    }

    /**
     * Increases the indentation level.
     */
    public void indent() {
        linePrefixLength += INDENT;
    }

    /**
     * Decreases the indentation level.
     */
    public void outdent() {
        linePrefixLength -= INDENT;
    }

    private String getLinePrefix() {
        return spaces(linePrefixLength);
    }

    /**
     * Returns a string of N spaces.
     * @param n Number of spaces
     * @return String of N spaces
     */
    private static synchronized String spaces(int n)
    {
        while (n > BIG_STRING.length()) {
            BIG_STRING = BIG_STRING + BIG_STRING;
        }
        return BIG_STRING.substring(0, n);
    }

    public void setParentArgs(Calc calc, Map<String, Object> argumentMap) {
        parentArgMap.put(calc, argumentMap);
    }

    /**
     * Whether to print out attributes relating to how a statement was actually
     * executed. If false, client should only send attributes relating to the
     * plan.
     *
     * @return Whether client should send attributes about profiling
     */
    public boolean enableProfiling() {
        return profiling;
    }
}

// End CalcWriter.java
