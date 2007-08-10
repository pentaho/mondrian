/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import java.util.List;
import java.util.Arrays;

/**
 * Enumeration of ways that a compiled expression can return its result to
 * its caller.
 *
 * @version $Id$
 * @author jhyde
 */
public enum ResultStyle {
    /**
     * Indicates that caller will accept any applicable style.
     */
    ANY,

    /**
     * Indicates that the expression returns its result as a list which may
     * safely be modified by the caller.
     */
    MUTABLE_LIST,

    /**
     * Indicates that the expression returns its result as a list which must
     * not be modified by the caller.
     */
    LIST,

    /**
     * Indicates that the expression returns its result as an Iterable
     * which must not be modified by the caller.
     */
    ITERABLE,

    /**
     * Indicates that the expression results its result as an immutable
     * value. This is typical for expressions which return string and
     * numeric values.
     */
    VALUE;

    // ---------------------------------------------------------------
    // There follow a set of convenience constants for commonly-used
    // collections of result styles.
    
    public static final List<ResultStyle> ANY_LIST =
        Arrays.asList(
            ANY);

    public static final List<ResultStyle> ITERABLE_ONLY =
        Arrays.asList(
            ITERABLE);

    public static final List<ResultStyle> MUTABLELIST_ONLY =
        Arrays.asList(
            MUTABLE_LIST);

    public static final List<ResultStyle> LIST_ONLY =
        Arrays.asList(
            LIST);

    public static final List<ResultStyle> ITERABLE_ANY =
        Arrays.asList(
            ITERABLE,
            ANY);

    public static final List<ResultStyle> ITERABLE_LIST =
        Arrays.asList(
            ITERABLE,
            LIST);

    public static final List<ResultStyle> ITERABLE_MUTABLELIST =
        Arrays.asList(
            ITERABLE,
            MUTABLE_LIST);

    public static final List<ResultStyle> ITERABLE_LIST_MUTABLELIST =
        Arrays.asList(
            ITERABLE,
            LIST,
            MUTABLE_LIST);

    public static final List<ResultStyle> LIST_MUTABLELIST =
        Arrays.asList(
            LIST,
            MUTABLE_LIST);

    public static final List<ResultStyle> MUTABLELIST_LIST =
        Arrays.asList(
            MUTABLE_LIST,
            LIST);

    public static final List<ResultStyle> ITERABLE_LIST_MUTABLELIST_ANY =
        Arrays.asList(
            ITERABLE,
            LIST,
            MUTABLE_LIST,
            ANY);

    public static final List<ResultStyle> ITERABLE_MUTABLELIST_LIST =
        Arrays.asList(
            ITERABLE,
            MUTABLE_LIST,
            LIST);
}

// End ResultStyle.java
