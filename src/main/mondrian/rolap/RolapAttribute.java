/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.Property;
import mondrian.olap.Util;
import mondrian.spi.Dialect;
import mondrian.spi.MemberFormatter;
import org.olap4j.metadata.Level;

import java.util.ArrayList;
import java.util.List;

/**
 * Attribute.
 *
 * @version $Id$
 * @author jhyde
 */
public class RolapAttribute {
//    public final RolapCubeDimension dimension;

    final String name;

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

    final boolean all;

    RolapAttribute parentAttribute;

    final org.olap4j.metadata.Level.Type levelType;
    private final int approxRowCount;

    final MemberFormatter memberFormatter;

    public RolapAttribute(
        String name,
        List<RolapSchema.PhysColumn> keyList,
        RolapSchema.PhysColumn nameExp,
        RolapSchema.PhysColumn captionExp,
        List<RolapSchema.PhysColumn> orderByList,
        MemberFormatter memberFormatter,
        String nullValue,
        boolean all,
        Level.Type levelType,
        int approxRowCount)
    {
        assert levelType != null;
        switch (levelType)  {
        case NULL:
        case ALL:
            break;
        default:
            if (!name.equals("Measures")) {
                assert name != null;
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
        this.all = all;
        this.levelType = levelType;
        this.approxRowCount = approxRowCount;
        if (orderByList != null) {
            this.orderByList = orderByList;
        } else {
            this.orderByList = this.keyList;
        }
    }

    void setParentExpr(RolapSchema.PhysExpr parentExpr)
    {
        assert !(all && parentExpr != null);
    }

    public List<RolapProperty> getProperties() {
        return properties;
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
