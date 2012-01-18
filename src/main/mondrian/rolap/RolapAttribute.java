/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2012 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.spi.Dialect;
import mondrian.spi.MemberFormatter;
import mondrian.util.CompositeList;

import org.apache.log4j.Logger;

import org.olap4j.metadata.Level;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Attribute.
 *
 * @version $Id$
 * @author jhyde
 */
public class RolapAttribute extends OlapElementBase {
    private static final Logger LOGGER = Logger.getLogger(RolapAttribute.class);

    // Add intrinsic property NAME.
    // TODO: make key, parent etc. all properties
    private static final List<RolapProperty> INTRINSIC_PROPERTIES =
        Arrays.asList(
            RolapLevel.NAME_PROPERTY);

    final String name;
    final String description;

    /**
     * The column (or columns) that yields the attribute's key. The columns may
     * be calculated.
     */
    public final List<RolapSchema.PhysColumn> keyList;

    /**
     * Ths column that gives the name of members of this level. If null,
     * members are named using the key expression.
     */
    protected final RolapSchema.PhysColumn nameExp;

    /**
     * The list of columns that are used to sort the attribute.
     */
    protected final List<RolapSchema.PhysColumn> orderByList;

    /**
     * The column or expression which yields the caption of the attribute.
     */
    protected final RolapSchema.PhysColumn captionExp;

    private final List<RolapProperty> properties =
        new ArrayList<RolapProperty>();

    /**
     * Value that indicates a null parent in a parent-child hierarchy. Typical
     * values are {@code null} and the string {@code "0"}.
     */
    final String nullValue;

    RolapAttribute parentAttribute;

    final org.olap4j.metadata.Level.Type levelType;
    private final int approxRowCount;

    final MemberFormatter memberFormatter;

    /**
     * Creates an attribute.
     *
     * <p>An attribute ought to have a dimension, but a few important attributes
     * ('all', 'null', 'measures') don't. See if we can do without it.</p>
     *
     * <p>Note that the name expression is required. If the user did not specify
     * a name in the schema file, the name expression will be the same as the
     * key expression. (Provided that the key has just one column.)</p>
     *
     * @param name Name
     * @param visible Whether visible in user-interface
     * @param caption Caption
     * @param description Description
     * @param keyList List of key columns
     * @param nameExp Name column
     * @param captionExp Caption column
     * @param orderByList Ordering columns
     * @param memberFormatter Formatter
     * @param nullValue Value used to represent null, e.g. "#null"
     * @param levelType Level type
     * @param approxRowCount Approximate number of instances of this attribute
     */
    public RolapAttribute(
        String name,
        boolean visible,
        String caption,
        String description,
        List<RolapSchema.PhysColumn> keyList,
        RolapSchema.PhysColumn nameExp,
        RolapSchema.PhysColumn captionExp,
        List<RolapSchema.PhysColumn> orderByList,
        MemberFormatter memberFormatter,
        String nullValue,
        Level.Type levelType,
        int approxRowCount)
    {
        this.visible = visible;
        this.caption = caption;
        this.description = description;
        assert levelType != null;
        assert name != null;
        switch (levelType)  {
        case NULL:
        case ALL:
            break;
        default:
            if (!name.equals("Measures")) {
                assert keyList != null;
                assert nameExp != null;
            }
        }
        this.name = name;
        this.keyList = keyList;
        this.nameExp = nameExp;
        this.captionExp = captionExp;
        this.memberFormatter = memberFormatter;
        this.nullValue = nullValue;
        this.levelType = levelType;
        this.approxRowCount = approxRowCount;
        if (orderByList != null) {
            this.orderByList = orderByList;
        } else {
            this.orderByList = this.keyList;
        }
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public String getUniqueName() {
        // REVIEW: Do we need this method? Can't implement unless attributes
        // have a fixed dimension.
        throw new UnsupportedOperationException();
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, Id.Segment s, MatchType matchType)
    {
        throw new UnsupportedOperationException();
    }

    public String getQualifiedName() {
        throw new UnsupportedOperationException();
    }

    public Hierarchy getHierarchy() {
        throw new UnsupportedOperationException();
    }

    public Dimension getDimension() {
        throw new UnsupportedOperationException();
    }

    public List<RolapProperty> getExplicitProperties() {
        return properties;
    }

    public List<RolapProperty> getProperties() {
        return CompositeList.of(
            INTRINSIC_PROPERTIES,
            properties);
    }

    public Property.Datatype getType() {
        throw new UnsupportedOperationException();
    }

    public Dialect.Datatype getDatatype() {
        Util.deprecated("obsolete method - use keyExpList types", false);
//        assert keyList.size() == 1;
        return keyList.get(0).datatype;
    }
}

// End RolapAttribute.java
