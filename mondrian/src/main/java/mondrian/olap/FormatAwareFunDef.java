/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 eazyBI
// All Rights Reserved.
*/

package mondrian.olap;

/**
 * Interface for functions that want to control which format string is
 * inferred for calculated members using this function.
 *
 * <p>When a calculated member has no explicit FORMAT_STRING and its
 * defining expression is a call to a function implementing this interface,
 * {@link Formula} will call {@link #getFormatExpIndex(Exp[])} to determine
 * which argument's format to use, instead of doing a depth-first walk
 * that picks the first measure it encounters.
 *
 * <p>Can be implemented by {@link FunDef} implementations directly
 * (e.g., Min/Max), or by {@link mondrian.spi.UserDefinedFunction}
 * implementations — the UDF adapter in UdfResolver delegates automatically.
 *
 * @since Mar 2026
 */
public interface FormatAwareFunDef {
    /**
     * Sentinel value indicating the function does not participate in
     * format-aware resolution (e.g., a UDF wrapper where the actual UDF
     * does not implement this interface). Formula will fall through to
     * the default format-finding behavior.
     */
    int NOT_PARTICIPATING = Integer.MIN_VALUE;

    /**
     * Returns the index of the argument whose format string should be
     * used for the result of this function call.
     *
     * <p>Return values:
     * <ul>
     * <li>{@code 0 <= index < args.length}: use the format from the
     *     argument at this index</li>
     * <li>{@code -1}: skip format inference from arguments entirely
     *     (useful when the function's result type differs from all
     *     argument types, e.g., DateDiffDays returns a number from
     *     two date arguments)</li>
     * <li>{@link #NOT_PARTICIPATING}: this function does not participate;
     *     fall through to the default format-finding behavior</li>
     * </ul>
     *
     * <p>Implementations must return one of the values above; other
     * values are rejected by an assertion in {@link Formula}.
     *
     * @param args the arguments to the function call
     * @return argument index, -1 to skip, or NOT_PARTICIPATING
     */
    int getFormatExpIndex(Exp[] args);
}
