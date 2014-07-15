/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.spi.Dialect;
import mondrian.spi.MemberFormatter;

import org.olap4j.metadata.Level;

import java.util.*;

/**
 * Attribute that is intrinsic to all dimensions, and therefore does not belong
 * to any.
 *
 * <p>When including it in a dimension, call the </p>
 *
 * <p>Attributes belong to {@link RolapHierarchy hierarchies} and are composed
 * to make {@link RolapLevel levels}.
 *
 * @author jhyde
 */
public class RolapSharedAttribute extends RolapAttributeImpl {
    public RolapSharedAttribute(
        String name,
        boolean visible,
        List<RolapSchema.PhysColumn> keyList,
        RolapSchema.PhysColumn nameExp,
        RolapSchema.PhysColumn captionExp,
        List<RolapSchema.PhysColumn> orderByList,
        MemberFormatter memberFormatter,
        Level.Type levelType,
        int approxRowCount)
    {
        super(
            name, visible, keyList, nameExp, captionExp, orderByList,
            memberFormatter, levelType, approxRowCount, Larders.EMPTY);
    }

    /**
     * Creates a copy of a shared attribute that lives in a particular
     * dimension.
     *
     * @param dimension Dimension
     * @return Dimension-specific attribute
     */
    public RolapAttribute inDimension(final RolapDimension dimension) {
        return new RolapCommonAttribute(this, dimension);
    }

    public RolapDimension getDimension() {
        throw new UnsupportedOperationException();
    }

    private static class DelegatingAttribute implements RolapAttribute {
        private final RolapAttribute delegate;

        public DelegatingAttribute(RolapAttribute delegate) {
            this.delegate = delegate;
        }

        public String toString() {
            return delegate.toString();
        }

        public String getUniqueName() {
            return delegate.getUniqueName();
        }

        public String getName() {
            return delegate.getName();
        }

        public String getDescription() {
            return delegate.getDescription();
        }

        public OlapElement lookupChild(
            SchemaReader schemaReader, Id.Segment s, MatchType matchType)
        {
            return delegate.lookupChild(schemaReader, s, matchType);
        }

        public String getQualifiedName() {
            return delegate.getQualifiedName();
        }

        public Hierarchy getHierarchy() {
            return delegate.getHierarchy();
        }

        public RolapDimension getDimension() {
            return delegate.getDimension();
        }

        public String getCaption() {
            return delegate.getCaption();
        }

        public String getLocalized(LocalizedProperty prop, Locale locale) {
            return delegate.getLocalized(prop, locale);
        }

        public boolean isVisible() {
            return delegate.isVisible();
        }

        public List<RolapProperty> getExplicitProperties() {
            return delegate.getExplicitProperties();
        }

        public List<RolapProperty> getProperties() {
            return delegate.getProperties();
        }

        public Property.Datatype getType() {
            return delegate.getType();
        }

        public Dialect.Datatype getDatatype() {
            return delegate.getDatatype();
        }

        public int getApproxRowCount() {
            return delegate.getApproxRowCount();
        }

        public List<RolapSchema.PhysColumn> getKeyList() {
            return delegate.getKeyList();
        }

        public RolapSchema.PhysColumn getNameExp() {
            return delegate.getNameExp();
        }

        public RolapSchema.PhysColumn getCaptionExp() {
            return delegate.getCaptionExp();
        }

        public List<RolapSchema.PhysColumn> getOrderByList() {
            return delegate.getOrderByList();
        }

        public Level.Type getLevelType() {
            return delegate.getLevelType();
        }

        public MemberFormatter getMemberFormatter() {
            return delegate.getMemberFormatter();
        }

        public Larder getLarder() {
            return delegate.getLarder();
        }
    }

    private static class RolapCommonAttribute extends DelegatingAttribute {
        private final RolapDimension dimension;

        RolapCommonAttribute(
            RolapSharedAttribute delegate,
            RolapDimension dimension)
        {
            super(delegate);
            this.dimension = dimension;
        }

        public RolapDimension getDimension() {
            return dimension;
        }
    }
}

// End RolapSharedAttribute.java
