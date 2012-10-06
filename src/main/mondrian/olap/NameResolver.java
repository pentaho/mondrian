/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.olap;

import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.IdentifierSegment;

import java.util.List;

/**
 * Resolves a list of segments (a parsed identifier) to an OLAP element.
 */
public final class NameResolver {

    /**
     * Creates a NameResolver.
     */
    public NameResolver() {
    }

    /**
     * Resolves a list of segments (a parsed identifier) to an OLAP element.
     *
     * @param parent Parent element to search in, usually a cube
     * @param segments Exploded compound name, such as {"Products",
     *   "Product Department", "Produce"}
     * @param failIfNotFound If the element is not found, determines whether
     *   to return null or throw an error
     * @param category Type of returned element, a {@link Category} value;
     *   {@link Category#Unknown} if it doesn't matter.
     * @param matchType Match type
     * @param namespaces Namespaces wherein to find child element at each step
     * @return OLAP element with given name, or null if not found
     */
    public OlapElement resolve(
        OlapElement parent,
        List<IdentifierSegment> segments,
        boolean failIfNotFound,
        int category,
        MatchType matchType,
        List<Namespace> namespaces)
    {
        OlapElement element;
        if (matchType == MatchType.EXACT) {
            element = resolveExact(
                parent,
                segments,
                namespaces);
        } else {
            element = resolveInexact(
                parent,
                segments,
                matchType,
                namespaces);
        }
        if (element != null) {
            element = nullify(category, element);
        }
        if (element == null && failIfNotFound) {
            throw Util.newElementNotFoundException(
                category,
                new IdentifierNode(segments));
        }
        return element;
    }

    private OlapElement resolveInexact(
        OlapElement parent,
        List<IdentifierSegment> segments,
        MatchType matchType,
        List<Namespace> namespaces)
    {
        OlapElement element = parent;
        segmentLoop:
        for (final IdentifierSegment segment : segments) {
            assert element != null;
            for (Namespace namespace : namespaces) {
                OlapElement child =
                    namespace.lookupChild(element, segment, matchType);
                if (child != null) {
                    switch (matchType) {
                    case EXACT:
                    case EXACT_SCHEMA:
                        break;
                    case BEFORE:
                        if (!Util.matches(segment, child.getName())) {
                            matchType = MatchType.LAST;
                        }
                        break;
                    case AFTER:
                        if (!Util.matches(segment, child.getName())) {
                            matchType = MatchType.FIRST;
                        }
                        break;
                    }
                    element = child;
                    continue segmentLoop;
                }
            }
            return null;
        }
        return element;
    }

    // same logic as resolveInexact, pared down for common case
    // matchType == EXACT
    private OlapElement resolveExact(
        OlapElement parent,
        List<IdentifierSegment> segments,
        List<Namespace> namespaces)
    {
        OlapElement element = parent;
        segmentLoop:
        for (final IdentifierSegment segment : segments) {
            assert element != null;
            for (Namespace namespace : namespaces) {
                OlapElement child = namespace.lookupChild(element, segment);
                if (child != null) {
                    element = child;
                    continue segmentLoop;
                }
            }
            return null;
        }
        return element;
    }

    /**
     * Converts an element to the required type, converting if possible,
     * returning null if it is not of the required type and cannot be converted.
     *
     * @param category Desired category of element
     * @param element Element
     * @return Element of the desired category, or null
     */
    private OlapElement nullify(int category, OlapElement element) {
        switch (category) {
        case Category.Unknown:
            return element;
        case Category.Member:
            return element instanceof Member ? element : null;
        case Category.Level:
            return element instanceof Level ? element : null;
        case Category.Hierarchy:
            if (element instanceof Hierarchy) {
                return element;
            } else if (element instanceof Dimension) {
                final Dimension dimension = (Dimension) element;
                if (dimension.getHierarchyList().size() == 1) {
                    return dimension.getHierarchyList().get(0);
                }
                return null;
            } else {
                return null;
            }
        case Category.Dimension:
            return element instanceof Dimension ? element : null;
        case Category.Set:
            return element instanceof NamedSet ? element : null;
        default:
            throw Util.newInternal("unexpected: " + category);
        }
    }

    /**
     * Returns whether a formula (representing a calculated member or named
     * set) matches a given parent and name segment.
     *
     * @param formula Formula
     * @param parent Parent element
     * @param segment Name segment
     * @return Whether formula matches
     */
    public static boolean matches(
        Formula formula,
        OlapElement parent,
        IdentifierSegment segment)
    {
        if (!Util.matches(segment, formula.getName())) {
            return false;
        }
        if (formula.isMember()) {
            final Member formulaMember = formula.getMdxMember();
            if (formulaMember.getParentMember() != null) {
                if (parent instanceof Member) {
                    // SSAS matches calc members very loosely. For example,
                    // [Foo].[Z] will match calc member [Foo].[X].[Y].[Z].
                    return formulaMember.getParentMember().isChildOrEqualTo(
                        (Member) parent);
                } else if (parent instanceof Hierarchy) {
                    return formulaMember.getParentMember().getHierarchy()
                        .equals(parent);
                } else {
                    return parent.getUniqueName().equals(
                        formulaMember.getParentMember().getUniqueName());
                }
            } else {
                // If parent is not a member, member must be a root member.
                return parent.equals(formulaMember.getHierarchy())
                    || parent.equals(formulaMember.getDimension())
                    || parent instanceof Cube
                    && !MondrianProperties.instance().NeedDimensionPrefix.get();
            }
        } else {
            return parent instanceof Cube;
        }
    }

    /**
     * Naming context within which elements are defined.
     *
     * <p>Elements' names are hierarchical, so elements are resolved one
     * name segment at a time. It is possible for an element to be defined
     * in a different namespace than its parent: for example, stored member
     * [Dim].[Hier].[X].[Y] might have a child [Dim].[Hier].[X].[Y].[Z] which
     * is a calculated member defined using a WITH MEMBER clause.</p>
     */
    public interface Namespace {
        /**
         * Looks up a child element, using a match type for inexact matching.
         *
         * <p>If {@code matchType} is {@link MatchType#EXACT}, effect is
         * identical to calling
         * {@link #lookupChild(OlapElement, org.olap4j.mdx.IdentifierSegment)}.</p>
         *
         * <p>Match type is ignored except when searching for members.</p>
         *
         * @param parent Parent element
         * @param segment Name segment
         * @param matchType Match type
         * @return Olap element, or null
         */
        OlapElement lookupChild(
            OlapElement parent,
            IdentifierSegment segment,
            MatchType matchType);

        /**
         * Looks up a child element.
         *
         * @param parent Parent element
         * @param segment Name segment
         * @return Olap element, or null
         */
        OlapElement lookupChild(
            OlapElement parent,
            IdentifierSegment segment);
    }
}

// End NameResolver.java
