/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianException;

import org.eigenbase.xom.Location;
import org.eigenbase.xom.NodeDef;

import java.util.List;

/**
 * Implementation of {@link RolapSchemaLoader.Handler}.
 *
 * <p>Derived class must implement {@link #getWarningList()}.</p>
 *
 * @author jhyde
 */
abstract class RolapSchemaLoaderHandlerImpl
    implements RolapSchemaLoader.Handler
{
    /**
     * Number of errors (messages logged via {@link #error}) encountered
     * during validation. If there are any errors, the schema is not viable
     * for queries. Fatal errors (logged via {@link #fatal}) will already
     * have aborted the validation process; warnings (logged via
     * {@link #warning}) will have been logged without incrementing the
     * error count.
     */
    private int errorCount;

    /**
     * Creates a HandlerImpl.
     */
    public RolapSchemaLoaderHandlerImpl() {
    }

    public RolapSchema.XmlLocation locate(NodeDef node, String attributeName) {
        if (node == null) {
            return null;
        }
        final Location location = node.getLocation();
        if (location == null) {
            return null;
        }
        return new RolapSchema.XmlLocationImpl(node, location, attributeName);
    }

    public void warning(
        String message,
        NodeDef node,
        String attributeName)
    {
        warning(message, node, attributeName, null);
    }

    public void warning(
        String message,
        NodeDef node,
        String attributeName,
        Throwable cause)
    {
        final RolapSchema.XmlLocation xmlLocation = locate(node, attributeName);
        final RolapSchema.MondrianSchemaException ex =
            new RolapSchema.MondrianSchemaException(
                message, describe(node), xmlLocation,
                RolapSchema.Severity.WARNING, cause);
        final List<RolapSchema.MondrianSchemaException> warningList =
            getWarningList();
        if (warningList != null) {
            warningList.add(ex);
        } else {
            throw ex;
        }
    }

    /**
     * Returns list where warnings are to be stored, or null if
     * warnings are to be escalated to errors and thrown immediately.
     *
     * @return Warning list
     */
    protected abstract List<RolapSchema.MondrianSchemaException>
        getWarningList();

    private String describe(NodeDef node) {
        // TODO: If node is not a namedElement, list its ancestors until we
        // hit a NamedElement. For example: Key in Dimension 'foo'.
        // Will require a new method DOMWrapper Annotator.getParent(DOMWrapper).
        if (node == null) {
            return null;
        } else if (node instanceof MondrianDef.NamedElement) {
            return node.getName()
                   + " '"
                   + ((MondrianDef.NamedElement) node).getNameAttribute()
                   + "'";
        } else {
            return node.getName();
        }
    }

    public void error(
        String message,
        NodeDef node,
        String attributeName)
    {
        final RolapSchema.XmlLocation xmlLocation = locate(node, attributeName);
        final Throwable cause = null;
        final RolapSchema.MondrianSchemaException ex =
            new RolapSchema.MondrianSchemaException(
                message, describe(node), xmlLocation,
                RolapSchema.Severity.ERROR, cause);
        final List<RolapSchema.MondrianSchemaException> warningList =
            getWarningList();
        if (warningList != null) {
            ++errorCount;
            warningList.add(ex);
        } else {
            throw ex;
        }
    }

    public void error(
        MondrianException message, NodeDef node, String attributeName)
    {
        error(message.toString(), node, attributeName);
    }

    public RuntimeException fatal(
        String message,
        NodeDef node,
        String attributeName)
    {
        final RolapSchema.XmlLocation xmlLocation = locate(node, attributeName);
        final Throwable cause = null;
        return new RolapSchema.MondrianSchemaException(
            message, describe(node), xmlLocation,
            RolapSchema.Severity.FATAL, cause);
    }

    public void check() {
        if (errorCount > 0) {
            throw new RolapSchemaLoader.MondrianMultipleSchemaException(
                "There were schema errors",
                getWarningList());
        }
    }
}

// End RolapSchemaLoaderHandlerImpl.java
