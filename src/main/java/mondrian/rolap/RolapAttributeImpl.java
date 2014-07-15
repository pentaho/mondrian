/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.spi.*;
import mondrian.spi.MemberFormatter;
import mondrian.util.CompositeList;

import org.apache.log4j.Logger;

import org.olap4j.metadata.Level;

import java.util.*;

/**
 * Default implementation of {@link RolapAttribute}.
 *
 * @author jhyde
 */
public abstract class RolapAttributeImpl
    extends OlapElementBase
    implements RolapAttribute
{
    // Add intrinsic property NAME.
    // TODO: make key, parent etc. all properties
    private static final List<RolapProperty> INTRINSIC_PROPERTIES =
        Collections.emptyList();

    final String name;
    private final Larder larder;

    public final List<RolapSchema.PhysColumn> keyList;

    protected final RolapSchema.PhysColumn nameExp;

    protected final List<RolapSchema.PhysColumn> orderByList;

    protected final RolapSchema.PhysColumn captionExp;

    private final List<RolapProperty> properties =
        new ArrayList<RolapProperty>();

    final Level.Type levelType;
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
     * @param keyList List of key columns
     * @param nameExp Name column
     * @param captionExp Caption column
     * @param orderByList Ordering columns
     * @param memberFormatter Formatter
     * @param levelType Level type
     * @param approxRowCount Approximate number of instances of this attribute
     * @param larder Larder
     */
    public RolapAttributeImpl(
        String name,
        boolean visible,
        List<RolapSchema.PhysColumn> keyList,
        RolapSchema.PhysColumn nameExp,
        RolapSchema.PhysColumn captionExp,
        List<RolapSchema.PhysColumn> orderByList,
        MemberFormatter memberFormatter,
        Level.Type levelType,
        int approxRowCount,
        Larder larder)
    {
        this.visible = visible;
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
        this.larder = larder;
        this.keyList = keyList;
        this.nameExp = nameExp;
        this.captionExp = captionExp;
        this.memberFormatter = memberFormatter;
        this.levelType = levelType;
        this.approxRowCount = approxRowCount;
        this.orderByList = orderByList;

        assert orderByList != null;
    }

    @Override
    public String toString() {
        return getName(); // can't call getUniqueName -- it throws
    }

    public List<RolapSchema.PhysColumn> getKeyList() {
        return keyList;
    }

    public RolapSchema.PhysColumn getNameExp() {
        return nameExp;
    }

    public RolapSchema.PhysColumn getCaptionExp() {
        return captionExp;
    }

    public List<RolapSchema.PhysColumn> getOrderByList() {
        return orderByList;
    }

    public Level.Type getLevelType() {
        return levelType;
    }

    public MemberFormatter getMemberFormatter() {
        return memberFormatter;
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public String getUniqueName() {
        return Util.makeFqName(getDimension(), name);
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return Larders.getDescription(larder);
    }

    public Larder getLarder() {
        return larder;
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

    public List<RolapProperty> getExplicitProperties() {
        return properties;
    }

    public List<RolapProperty> getProperties() {
        //noinspection unchecked
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

    public int getApproxRowCount() {
        return approxRowCount;
    }
}

// End RolapAttributeImpl.java
