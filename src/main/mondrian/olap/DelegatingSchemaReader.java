/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2006 Julian Hyde
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 26, 2003
*/
package mondrian.olap;

import mondrian.calc.Calc;

import javax.sql.DataSource;
import java.util.List;

/**
 * <code>DelegatingSchemaReader</code> implements {@link SchemaReader} by
 * delegating all methods to an underlying {@link SchemaReader}.
 *
 * It is a convenient base class if you want to override just a few of
 * {@link SchemaReader}'s methods.
 *
 * @author jhyde
 * @since Feb 26, 2003
 * @version $Id$
 */
public abstract class DelegatingSchemaReader implements SchemaReader {
    protected final SchemaReader schemaReader;

    DelegatingSchemaReader(SchemaReader schemaReader) {
        this.schemaReader = schemaReader;
    }

    public Role getRole() {
        return schemaReader.getRole();
    }

    public Member[] getHierarchyRootMembers(Hierarchy hierarchy) {
        return schemaReader.getHierarchyRootMembers(hierarchy);
    }

    public Member getMemberParent(Member member) {
        return schemaReader.getMemberParent(member);
    }

    public Member[] getMemberChildren(Member member) {
        return schemaReader.getMemberChildren(member);
    }

    public Member[] getMemberChildren(Member[] members) {
        return schemaReader.getMemberChildren(members);
    }

    public int getMemberDepth(Member member) {
        return schemaReader.getMemberDepth(member);
    }

    public Member getMemberByUniqueName(
        String[] uniqueNameParts,
        boolean failIfNotFound)
    {
        return getMemberByUniqueName(
            uniqueNameParts, failIfNotFound, MatchType.EXACT);
    }

    public Member getMemberByUniqueName(
            String[] uniqueNameParts,
            boolean failIfNotFound,
            MatchType matchType)
    {
        return schemaReader.getMemberByUniqueName(
                uniqueNameParts, failIfNotFound, matchType);
    }

    public OlapElement lookupCompound(
        OlapElement parent, String[] names,
        boolean failIfNotFound, int category)
    {
        return lookupCompound(
            parent, names, failIfNotFound, category, MatchType.EXACT);
    }

    public OlapElement lookupCompound(
            OlapElement parent, String[] names,
            boolean failIfNotFound, int category, MatchType matchType) {
        return schemaReader.lookupCompound(
                parent, names, failIfNotFound, category, matchType);
    }

    public Member getCalculatedMember(String[] nameParts) {
        return schemaReader.getCalculatedMember(nameParts);
    }

    public NamedSet getNamedSet(String[] nameParts) {
        return schemaReader.getNamedSet(nameParts);
    }

    public void getMemberRange(
            Level level,
            Member startMember,
            Member endMember,
            List<Member> list) {
        schemaReader.getMemberRange(level, startMember, endMember, list);
    }

    public Member getLeadMember(Member member, int n) {
        return schemaReader.getLeadMember(member, n);
    }

    public int compareMembersHierarchically(Member m1, Member m2) {
        return schemaReader.compareMembersHierarchically(m1, m2);
    }

    public OlapElement getElementChild(OlapElement parent, String name) {
        return getElementChild(parent, name, MatchType.EXACT);
    }

    public OlapElement getElementChild(
        OlapElement parent, String name, MatchType matchType)
    {
        return schemaReader.getElementChild(parent, name, matchType);
    }

    public Member[] getLevelMembers(Level level, boolean includeCalculated) {
        return schemaReader.getLevelMembers(level, includeCalculated);
    }

    public Level[] getHierarchyLevels(Hierarchy hierarchy) {
        return schemaReader.getHierarchyLevels(hierarchy);
    }

    public Member getHierarchyDefaultMember(Hierarchy hierarchy) {
        return schemaReader.getHierarchyDefaultMember(hierarchy);
    }

    public boolean isDrillable(Member member) {
        return schemaReader.isDrillable(member);
    }

    public boolean isVisible(Member member) {
        return schemaReader.isVisible(member);
    }

    public Cube[] getCubes() {
        return schemaReader.getCubes();
    }

    public List<Member> getCalculatedMembers(Hierarchy hierarchy) {
        return schemaReader.getCalculatedMembers(hierarchy);
    }

    public List<Member> getCalculatedMembers(Level level) {
        return schemaReader.getCalculatedMembers(level);
    }

    public List<Member> getCalculatedMembers() {
        return schemaReader.getCalculatedMembers();
    }

    public int getChildrenCountFromCache(Member member) {
        return schemaReader.getChildrenCountFromCache(member);
    }

    public int getLevelCardinality(
        Level level, boolean approximate, boolean materialize)
    {
        return schemaReader.getLevelCardinality(
            level, approximate, materialize);
    }

    public Member[] getLevelMembers(Level level, Evaluator context) {
      return schemaReader.getLevelMembers(level, context);
    }

    public Member[] getMemberChildren(Member member, Evaluator context) {
      return schemaReader.getMemberChildren(member, context);
    }

    public Member[] getMemberChildren(Member[] members, Evaluator context) {
      return schemaReader.getMemberChildren(members, context);
    }

    public Member lookupMemberChildByName(Member member, String memberName) {
        return lookupMemberChildByName(member, memberName, MatchType.EXACT);
    }

    public Member lookupMemberChildByName(
        Member member, String memberName, MatchType matchType)
    {
        return schemaReader.lookupMemberChildByName(
            member, memberName, matchType);
    }

    public NativeEvaluator getNativeSetEvaluator(
            FunDef fun, Exp[] args, Evaluator evaluator, Calc calc) {
        return schemaReader.getNativeSetEvaluator(fun, args, evaluator, calc);
    }

    public Parameter getParameter(String name) {
        return schemaReader.getParameter(name);
    }

    public DataSource getDataSource() {
        return schemaReader.getDataSource();
    }
}

// End DelegatingSchemaReader.java
