/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import org.olap4j.metadata.*;
import org.olap4j.OlapException;
import org.olap4j.impl.*;

import java.util.*;

/**
 * Implementation of {@link Cube}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @version $Id$
 * @since May 24, 2007
 */
class MondrianOlap4jCube implements Cube, Named {
    private final mondrian.olap.Cube cube;
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
        for (mondrian.olap.Dimension dimension : cube.getDimensions()) {
            list.add(
                new MondrianOlap4jDimension(
                    olap4jSchema, dimension));
        }
        return Olap4jUtil.cast(list);
    }

    public NamedList<Hierarchy> getHierarchies() {
        NamedList<MondrianOlap4jHierarchy> list =
            new NamedListImpl<MondrianOlap4jHierarchy>();
        for (mondrian.olap.Dimension dimension : cube.getDimensions()) {
            for (mondrian.olap.Hierarchy hierarchy : dimension.getHierarchies()) {
                list.add(
                    new MondrianOlap4jHierarchy(
                        olap4jSchema, hierarchy));
            }
        }
        return Olap4jUtil.cast(list);
    }

    public List<Measure> getMeasures() {
        final MondrianOlap4jLevel measuresLevel =
            (MondrianOlap4jLevel)
                getDimensions().get("Measures").getDefaultHierarchy()
                    .getLevels().get(0);
        return Olap4jUtil.cast(measuresLevel.getMembers());
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

    public String getCaption(Locale locale) {
        // todo: i81n
        return cube.getCaption();
    }

    public String getDescription(Locale locale) {
        // todo: i81n
        return cube.getDescription();
    }

    public MondrianOlap4jMember lookupMember(String... nameParts) {
        final MondrianOlap4jConnection olap4jConnection =
            olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        final mondrian.olap.SchemaReader schemaReader =
            cube.getSchemaReader(olap4jConnection.connection.getRole());

        final List<mondrian.olap.Id.Segment> segmentList =
            new ArrayList<mondrian.olap.Id.Segment>();
        for (String namePart : nameParts) {
            segmentList.add(
                new mondrian.olap.Id.Segment(
                    namePart, mondrian.olap.Id.Quoting.QUOTED));
        }
        final mondrian.olap.Member member =
            schemaReader.getMemberByUniqueName(segmentList, false);
        if (member == null) {
            return null;
        }
        return olap4jConnection.toOlap4j(member);
    }

    public List<Member> lookupMembers(
        Set<Member.TreeOp> treeOps,
        String... nameParts) throws OlapException
    {
        final MondrianOlap4jMember member = lookupMember(nameParts);
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
                m = m.getParentMember()) {
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
                siblingMembers = parentMember.getChildMembers();
            } else {
                siblingMembers =
                    Olap4jUtil.cast(member.getHierarchy().getRootMembers());
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
            for (MondrianOlap4jMember childMember : member.getChildMembers()) {
                list.add(childMember);
                addDescendants(list, childMember);
            }
        } else if (treeOps.contains(Member.TreeOp.CHILDREN)) {
            for (MondrianOlap4jMember childMember : member.getChildMembers()) {
                list.add(childMember);
            }
        }
        // Lastly, add siblings which occur after the member itself. They
        // occur after all of the descendants in the hierarchical ordering.
        if (remainingSiblingsList != null) {
            list.addAll(remainingSiblingsList);
        }
        return Olap4jUtil.cast(list);
    }

    private static void addDescendants(
        List<MondrianOlap4jMember> list,
        MondrianOlap4jMember member)
    {
        for (MondrianOlap4jMember childMember : member.getChildMembers()) {
            list.add(childMember);
            addDescendants(list, childMember);
        }
    }
}

// End MondrianOlap4jCube.java
