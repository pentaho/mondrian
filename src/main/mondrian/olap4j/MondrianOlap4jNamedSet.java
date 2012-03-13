/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.olap4j;

import mondrian.olap.OlapElement;

import org.olap4j.impl.Named;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.NamedSet;

/**
 * Implementation of {@link org.olap4j.metadata.NamedSet}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @since Nov 12, 2007
 */
class MondrianOlap4jNamedSet
    extends MondrianOlap4jMetadataElement
    implements NamedSet, Named
{
    private final MondrianOlap4jCube olap4jCube;
    private mondrian.olap.NamedSet namedSet;

    MondrianOlap4jNamedSet(
        MondrianOlap4jCube olap4jCube,
        mondrian.olap.NamedSet namedSet)
    {
        this.olap4jCube = olap4jCube;
        this.namedSet = namedSet;
    }

    public Cube getCube() {
        return olap4jCube;
    }

    public ParseTreeNode getExpression() {
        final MondrianOlap4jConnection olap4jConnection =
            olap4jCube.olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData
                .olap4jConnection;
        return olap4jConnection.toOlap4j(namedSet.getExp());
    }

    public String getName() {
        return namedSet.getName();
    }

    public String getUniqueName() {
        return namedSet.getUniqueName();
    }

    public String getCaption() {
        return namedSet.getLocalized(
            OlapElement.LocalizedProperty.CAPTION,
            olap4jCube.olap4jSchema.getLocale());
    }

    public String getDescription() {
        return namedSet.getLocalized(
            OlapElement.LocalizedProperty.DESCRIPTION,
            olap4jCube.olap4jSchema.getLocale());
    }

    public boolean isVisible() {
        return namedSet.isVisible();
    }

    protected OlapElement getOlapElement() {
        return namedSet;
    }
}

// End MondrianOlap4jNamedSet.java
