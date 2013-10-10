/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.Calc;
import mondrian.olap.*;

import java.util.*;

/**
 * Implementation of {@link mondrian.rolap.RolapMember} that delegates all calls
 * to an underlying member.
 *
 * @author jhyde
 * @since Mar 16, 2010
 */
public class DelegatingRolapMember implements RolapMember {
    protected final RolapMember member;

    protected DelegatingRolapMember(RolapMember member) {
        this.member = member;
    }

    public RolapCubeLevel getLevel() {
        return member.getLevel();
    }

    public Comparable getKey() {
        return member.getKey();
    }

    public List<Comparable> getKeyAsList() {
        return member.getKeyAsList();
    }

    public Object[] getKeyAsArray() {
        return member.getKeyAsArray();
    }

    public Comparable getKeyCompact() {
        return member.getKeyCompact();
    }

    public RolapMember getParentMember() {
        return member.getParentMember();
    }

    public RolapCubeHierarchy getHierarchy() {
        return member.getHierarchy();
    }

    public String getParentUniqueName() {
        return member.getParentUniqueName();
    }

    public MemberType getMemberType() {
        return member.getMemberType();
    }

    public boolean isParentChildLeaf() {
        return member.isParentChildLeaf();
    }

    public void setName(String name) {
        member.setName(name);
    }

    public boolean isAll() {
        return member.isAll();
    }

    public boolean isMeasure() {
        return member.isMeasure();
    }

    public boolean isNull() {
        return member.isNull();
    }

    public boolean isChildOrEqualTo(Member member2) {
        return member.isChildOrEqualTo(member2);
    }

    public boolean isCalculated() {
        return member.isCalculated();
    }

    public boolean isEvaluated() {
        return member.isEvaluated();
    }

    public int getSolveOrder() {
        return member.getSolveOrder();
    }

    public Exp getExpression() {
        return member.getExpression();
    }

    public List<Member> getAncestorMembers() {
        return member.getAncestorMembers();
    }

    public boolean isCalculatedInQuery() {
        return member.isCalculatedInQuery();
    }

    public Object getPropertyValue(Property property) {
        return member.getPropertyValue(property);
    }

    public String getPropertyFormattedValue(Property property) {
        return member.getPropertyFormattedValue(property);
    }

    public void setProperty(Property property, Object value) {
        member.setProperty(property, value);
    }

    public Property[] getProperties() {
        return member.getProperties();
    }

    public int getOrdinal() {
        return member.getOrdinal();
    }

    public Comparable getOrderKey() {
        return member.getOrderKey();
    }

    public boolean isHidden() {
        return member.isHidden();
    }

    public int getDepth() {
        return member.getDepth();
    }

    public RolapMember getDataMember() {
        return member.getDataMember();
    }

    @SuppressWarnings({"unchecked"})
    public int compareTo(Object o) {
        return member.compareTo(o);
    }

    public String getUniqueName() {
        return member.getUniqueName();
    }

    public String getName() {
        return member.getName();
    }

    public String getDescription() {
        return member.getDescription();
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, Id.Segment s, MatchType matchType)
    {
        return member.lookupChild(schemaReader, s, matchType);
    }

    public Larder getLarder() {
        return member.getLarder();
    }

    public String getQualifiedName() {
        return member.getQualifiedName();
    }

    public RolapCubeDimension getDimension() {
        return member.getDimension();
    }

    public RolapCube getCube() {
        return member.getCube();
    }

    public Object getPropertyValue(String propertyName) {
        return member.getPropertyValue(propertyName);
    }

    public Object getPropertyValue(String propertyName, boolean matchCase) {
        return member.getPropertyValue(propertyName, matchCase);
    }

    public String getPropertyFormattedValue(String propertyName) {
        return member.getPropertyFormattedValue(propertyName);
    }

    public void setProperty(String propertyName, Object value) {
        member.setProperty(propertyName, value);
    }

    public Map<String, Annotation> getAnnotationMap() {
        return member.getAnnotationMap();
    }

    public String getCaption() {
        return member.getCaption();
    }

    public String getLocalized(LocalizedProperty prop, Locale locale) {
        return member.getLocalized(prop, locale);
    }

    public boolean isVisible() {
        return member.isVisible();
    }

    public void setContextIn(RolapEvaluator evaluator) {
        member.setContextIn(evaluator);
    }

    public int getHierarchyOrdinal() {
        return member.getHierarchyOrdinal();
    }

    public Calc getCompiledExpression(RolapEvaluatorRoot root) {
        return member.getCompiledExpression(root);
    }

    public boolean containsAggregateFunction() {
        return member.containsAggregateFunction();
    }
}

// End DelegatingRolapMember.java
