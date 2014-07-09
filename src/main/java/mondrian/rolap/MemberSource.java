/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
//
// jhyde, 21 December, 2001
*/
package mondrian.rolap;

import mondrian.olap.Id;

import java.util.List;

/**
 * A <code>MemberSource</code> has the basic operations to read the members of a
 * {@link RolapHierarchy hierarchy}.
 *
 * <p>A <code>MemberSource</code> may optionally support writeback to a
 * {@link MemberCache}. During the initialization of a
 * <code>MemberSource</code>, the consumer calls {@link #setCache}; the return
 * value indicates whether the <code>MemberSource</code> supports
 * cache-writeback.
 *
 * <p>A <dfn>custom member reader</dfn> is a user-defined class which implements
 * the operations to retrieve members. It either implements the
 * <code>MemberSource</code> interface, or the derived interface
 * {@link MemberReader}, which has more operations. In addition to the interface
 * methods, the class must have a constructor which takes parameters
 * <code>({@link RolapHierarchy}, {@link java.util.Properties})</code> and
 * throws no exceptions. To declare a hierarchy based upon the class, use the
 * <code>memberReaderClass</code> attribute of the
 * <code>&lt;Hierarchy&gt;</code> element in your XML schema file; the
 * <code>properties</code> constructor parameter is populated from any
 * <ocde>&lt;Param name="..." value="..."&gt;</code> child elements.
 *
 * @see MemberReader
 * @see MemberCache
 *
 * @author jhyde
 * @since 21 December, 2001
 */
public interface MemberSource {
    /**
     * Returns the hierarchy that this source is reading for.
     */
    RolapCubeHierarchy getHierarchy();

    /**
     * Sets the cache which this <code>MemberSource</code> will write to.
     *
     * <p>Cache-writeback is optional (for example, {@link SqlMemberSource}
     * supports it, and {@link ArrayMemberSource} does not), and the return
     * value from this method indicates whether this object supports it.
     *
     * <p>If this method returns <code>true</code>, the {@link #getMembers},
     * {@link #getRootMembers} and {@link #getMemberChildren} methods must
     * write to the cache, in addition to returning members as a return value.
     *
     * @param cache The <code>MemberCache</code> which the caller would like
     *   this <code>MemberSource</code> to write to.
     * @return Whether this <code>MemberSource</code> supports cache-writeback.
     */
    boolean setCache(MemberCache cache);

    /**
     * Returns all members of this hierarchy, sorted by ordinal.
     *
     * <p>If this object {@link #setCache supports cache-writeaback}, also
     * writes these members to the cache.
     */
    List<RolapMember> getMembers();
    /**
     * Returns all members of this hierarchy which do not have a parent,
     * sorted by ordinal.
     *
     * <p>If this object {@link #setCache supports cache-writeback}, also
     * writes these members to the cache.
     *
     * @return {@link List} of {@link RolapMember}s
     */
    List<RolapMember> getRootMembers();

    /**
     * Writes all children <code>parentMember</code> to <code>children</code>.
     *
     * <p>If this object {@link #setCache supports cache-writeback}, also
     * writes these members to the cache.
     */
    void getMemberChildren(
        RolapMember parentMember,
        List<RolapMember> children);

    /**
     * Returns all members which are a child of one of the members in
     * <code>parentMembers</code>, sorted by ordinal.
     *
     * <p>If this object {@link #setCache supports cache-writeaback}, also
     * writes these members to the cache.
     */
    void getMemberChildren(
        List<RolapMember> parentMembers,
        List<RolapMember> children);

    /**
     * Returns an estimate of number of members in this hierarchy.
     */
    int getMemberCount();

    /**
     * Finds a member based upon its unique name.
     */
    RolapMember lookupMember(
        List<Id.Segment> uniqueNameParts,
        boolean failIfNotFound);
}

// End MemberSource.java
