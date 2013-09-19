/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap4j;

import mondrian.olap.*;

import org.olap4j.OlapException;
import org.olap4j.impl.*;
import org.olap4j.mdx.IdentifierSegment;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.*;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedSet;
import org.olap4j.metadata.Schema;

import java.util.*;

/**
 * Implementation of {@link Cube}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @since May 24, 2007
 */
class MondrianOlap4jCube
    extends MondrianOlap4jMetadataElement
    implements Cube, Named
{
    final mondrian.olap.Cube cube;
    final MondrianOlap4jSchema olap4jSchema;

    MondrianOlap4jCube(
        mondrian.olap.Cube cube,
        MondrianOlap4jSchema olap4jSchema)
    {
        this.cube = cube;
        this.olap4jSchema = olap4jSchema;
    }

    public Schema getSchema() {
        return olap4jSchema;
    }

    public int hashCode() {
        return olap4jSchema.hashCode()
            ^ cube.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj instanceof MondrianOlap4jCube) {
            MondrianOlap4jCube that = (MondrianOlap4jCube) obj;
            return this.olap4jSchema == that.olap4jSchema
                && this.cube.equals(that.cube);
        }
        return false;
    }

    public NamedList<Dimension> getDimensions() {
        NamedList<MondrianOlap4jDimension> list =
            new NamedListImpl<MondrianOlap4jDimension>();
        final MondrianOlap4jConnection olap4jConnection =
            olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        final mondrian.olap.SchemaReader schemaReader =
            olap4jConnection.getMondrianConnection2().getSchemaReader()
            .withLocus();
        for (mondrian.olap.Dimension dimension
            : schemaReader.getCubeDimensions(cube))
        {
            list.add(
                new MondrianOlap4jDimension(
                    olap4jSchema, dimension));
        }
        return Olap4jUtil.cast(list);
    }

    public NamedList<Hierarchy> getHierarchies() {
        NamedList<MondrianOlap4jHierarchy> list =
            new NamedListImpl<MondrianOlap4jHierarchy>();
        final MondrianOlap4jConnection olap4jConnection =
            olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        final mondrian.olap.SchemaReader schemaReader =
            olap4jConnection.getMondrianConnection2().getSchemaReader()
            .withLocus();
        for (mondrian.olap.Dimension dimension
            : schemaReader.getCubeDimensions(cube))
        {
            for (mondrian.olap.Hierarchy hierarchy
                : schemaReader.getDimensionHierarchies(dimension))
            {
                list.add(
                    new MondrianOlap4jHierarchy(
                        olap4jSchema, hierarchy));
            }
        }
        return Olap4jUtil.cast(list);
    }

    public List<Measure> getMeasures() {
        final Dimension dimension = getDimensions().get("Measures");
        if (dimension == null) {
            return Collections.emptyList();
        }
        final MondrianOlap4jConnection olap4jConnection =
            olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        try {
            final mondrian.olap.SchemaReader schemaReader =
                olap4jConnection.getMondrianConnection().getSchemaReader()
                .withLocus();
            final MondrianOlap4jLevel measuresLevel =
                (MondrianOlap4jLevel)
                    dimension.getDefaultHierarchy()
                        .getLevels().get(0);
            final List<Measure> measures =
                new ArrayList<Measure>();
            List<mondrian.olap.Member> levelMembers =
                schemaReader.getLevelMembers(
                    measuresLevel.level,
                    true);
            for (mondrian.olap.Member member : levelMembers) {
                // This corrects MONDRIAN-1123, a ClassCastException (see below)
                // that occurs when you create a calculated member on a
                // dimension other than Measures:
                // java.lang.ClassCastException:
                // mondrian.olap4j.MondrianOlap4jMember cannot be cast to
                // org.olap4j.metadata.Measure
                MondrianOlap4jMember olap4jMember = olap4jConnection.toOlap4j(
                    member);
                if (olap4jMember instanceof Measure) {
                    measures.add((Measure) olap4jMember);
                }
            }
            return measures;
        } catch (OlapException e) {
            // OlapException not possible, since measures are stored in memory.
            // Demote from checked to unchecked exception.
            throw new RuntimeException(e);
        }
    }

    public NamedList<NamedSet> getSets() {
        final NamedListImpl<MondrianOlap4jNamedSet> list =
            new NamedListImpl<MondrianOlap4jNamedSet>();
        final MondrianOlap4jConnection olap4jConnection =
            olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        for (mondrian.olap.NamedSet namedSet : cube.getNamedSets()) {
            list.add(olap4jConnection.toOlap4j(cube, namedSet));
        }
        return Olap4jUtil.cast(list);
    }

    public Collection<Locale> getSupportedLocales() {
        throw new UnsupportedOperationException();
    }

    public String getName() {
        return cube.getName();
    }

    public String getUniqueName() {
        return cube.getUniqueName();
    }

    public String getCaption() {
        return cube.getLocalized(
            OlapElement.LocalizedProperty.CAPTION, olap4jSchema.getLocale());
    }

    public String getDescription() {
        return cube.getLocalized(
            OlapElement.LocalizedProperty.DESCRIPTION,
            olap4jSchema.getLocale());
    }

    public boolean isVisible() {
        return cube.isVisible();
    }

    public MondrianOlap4jMember lookupMember(
        List<IdentifierSegment> nameParts)
        throws OlapException
    {
        final MondrianOlap4jConnection olap4jConnection =
            olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        final Role role = olap4jConnection.getMondrianConnection().getRole();
        final SchemaReader schemaReader =
            cube.getSchemaReader(role).withLocus();
        return lookupMember(schemaReader, nameParts);
    }

    private MondrianOlap4jMember lookupMember(
        SchemaReader schemaReader,
        List<IdentifierSegment> nameParts)
    {
        final List<mondrian.olap.Id.Segment> segmentList =
            new ArrayList<mondrian.olap.Id.Segment>();
        for (IdentifierSegment namePart : nameParts) {
            segmentList.add(Util.convert(namePart));
        }
        final mondrian.olap.Member member =
            schemaReader.getMemberByUniqueName(segmentList, false);
        if (member == null) {
            return null;
        }

        return olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData
            .olap4jConnection.toOlap4j(member);
    }

    public List<Member> lookupMembers(
        Set<Member.TreeOp> treeOps,
        List<IdentifierSegment> nameParts) throws OlapException
    {
        final MondrianOlap4jConnection olap4jConnection =
            olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        final Role role = olap4jConnection.getMondrianConnection().getRole();
        final SchemaReader schemaReader =
            cube.getSchemaReader(role).withLocus();
        final MondrianOlap4jMember member =
            lookupMember(schemaReader, nameParts);
        if (member == null) {
            return Collections.emptyList();
        }

        // Add ancestors and/or the parent. Ancestors are prepended, to ensure
        // hierarchical order.
        final List<MondrianOlap4jMember> list =
            new ArrayList<MondrianOlap4jMember>();
        if (treeOps.contains(Member.TreeOp.ANCESTORS)) {
            for (MondrianOlap4jMember m = member.getParentMember();
                m != null;
                m = m.getParentMember())
            {
                list.add(0, m);
            }
        } else if (treeOps.contains(Member.TreeOp.PARENT)) {
            final MondrianOlap4jMember parentMember = member.getParentMember();
            if (parentMember != null) {
                list.add(parentMember);
            }
        }

        // Add siblings. Siblings which occur after the member are deferred,
        // because they occur after children and descendants in the
        // hierarchical ordering.
        List<MondrianOlap4jMember> remainingSiblingsList = null;
        if (treeOps.contains(Member.TreeOp.SIBLINGS)) {
            final MondrianOlap4jMember parentMember = member.getParentMember();
            NamedList<MondrianOlap4jMember> siblingMembers;
            if (parentMember != null) {
                siblingMembers =
                    olap4jConnection.toOlap4j(
                        schemaReader.getMemberChildren(parentMember.member));
            } else {
                siblingMembers =
                    olap4jConnection.toOlap4j(
                        schemaReader.getHierarchyRootMembers(
                            member.member.getHierarchy()));
            }
            List<MondrianOlap4jMember> targetList = list;
            for (MondrianOlap4jMember siblingMember : siblingMembers) {
                if (siblingMember.equals(member)) {
                    targetList =
                        remainingSiblingsList =
                            new ArrayList<MondrianOlap4jMember>();
                } else {
                    targetList.add(siblingMember);
                }
            }
        }

        // Add the member itself.
        if (treeOps.contains(Member.TreeOp.SELF)) {
            list.add(member);
        }

        // Add descendants and/or children.
        if (treeOps.contains(Member.TreeOp.DESCENDANTS)) {
            addDescendants(list, schemaReader, olap4jConnection, member, true);
        } else if (treeOps.contains(Member.TreeOp.CHILDREN)) {
            addDescendants(list, schemaReader, olap4jConnection, member, false);
        }
        // Lastly, add siblings which occur after the member itself. They
        // occur after all of the descendants in the hierarchical ordering.
        if (remainingSiblingsList != null) {
            list.addAll(remainingSiblingsList);
        }
        return Olap4jUtil.cast(list);
    }

    private void addDescendants(
        List<MondrianOlap4jMember> list,
        SchemaReader schemaReader,
        MondrianOlap4jConnection olap4jConnection,
        MondrianOlap4jMember member,
        boolean recurse)
    {
        for (mondrian.olap.Member m
            : schemaReader.getMemberChildren(member.member))
        {
            MondrianOlap4jMember childMember = olap4jConnection.toOlap4j(m);
            list.add(childMember);
            if (recurse) {
                addDescendants(
                    list, schemaReader, olap4jConnection, childMember, recurse);
            }
        }
    }

    public boolean isDrillThroughEnabled() {
        return true;
    }

    protected OlapElement getOlapElement() {
        return cube;
    }
}

// End MondrianOlap4jCube.java
