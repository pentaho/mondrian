/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.spi.MemberFormatter;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * <code>RolapLevel</code> implements {@link Level} for a ROLAP database.
 *
 * <p>NOTE: This class must not contain any references to XML (MondrianDef)
 * objects. Put those in {@link mondrian.rolap.RolapSchemaLoader}.
 *
 * @author jhyde
 * @since 10 August, 2001
 */
public class RolapLevel extends LevelBase {

    private static final Logger LOGGER = Logger.getLogger(RolapLevel.class);

    protected RolapAttribute attribute;

    private final List<RolapProperty> inheritedProperties =
        new ArrayList<RolapProperty>();

    /** Condition under which members are hidden. (For ragged hierarchies.) */
    private final HideMemberCondition hideMemberCondition;

    /** Condition under which parent key is considered null. (For parent-child
     * hierarchies.) */
    final String nullParentValue;
    final RolapClosure closure;
    final RolapAttribute parentAttribute;
    private final Larder larder;
    final Map<String, List<Larders.Resource>> resourceMap;

    private final List<RolapSchema.PhysColumn> orderByList;

    static final RolapProperty KEY_PROPERTY =
        new RolapProperty(
            Property.KEY.name, null, null, Property.Datatype.TYPE_STRING, null,
            true, Larders.EMPTY);

    static final RolapProperty NAME_PROPERTY =
        new RolapProperty(
            Property.NAME.name, null, null, Property.Datatype.TYPE_STRING, null,
            true, Larders.EMPTY);

    // TODO: proper name
    static final RolapProperty ORDINAL_PROPERTY =
        new RolapProperty(
            "$ordinal", null, null, Property.Datatype.TYPE_STRING, null,
            true, Larders.EMPTY);

    /**
     * Creates a level.
     *
     * @param nullParentValue Value used to represent null, e.g. "#null"
     * @param closure Closure table
     */
    RolapLevel(
        RolapHierarchy hierarchy,
        String name,
        boolean visible,
        int depth,
        RolapAttribute attribute,
        RolapAttribute parentAttribute,
        List<RolapSchema.PhysColumn> orderByList,
        String nullParentValue,
        RolapClosure closure,
        HideMemberCondition hideMemberCondition,
        Larder larder,
        Map<String, List<Larders.Resource>> resourceMap)
    {
        super(hierarchy, name, visible, depth);
        this.larder = larder;
        this.resourceMap = resourceMap;
        this.attribute = attribute;
        this.orderByList = orderByList;
        this.parentAttribute = parentAttribute;
        this.nullParentValue = nullParentValue;
        this.closure = closure;
        this.hideMemberCondition = hideMemberCondition;

        assert larder != null;
        assert orderByList != null;
        assert hideMemberCondition != null;
        assert parentAttribute != null || nullParentValue == null;
        assert parentAttribute != null || closure == null;
    }

    public org.olap4j.metadata.Level.Type getLevelType() {
        return attribute.getLevelType();
    }

    public RolapHierarchy getHierarchy() {
        return (RolapHierarchy) hierarchy;
    }

    public Larder getLarder() {
        return larder;
    }

    public MemberFormatter getMemberFormatter() {
        return attribute.getMemberFormatter();
    }

    // override with refined return type
    public RolapLevel getParentLevel() {
        return (RolapLevel) super.getParentLevel();
    }

    // override with refined return type
    public RolapLevel getChildLevel() {
        return (RolapLevel) super.getChildLevel();
    }

    public RolapDimension getDimension() {
        return (RolapDimension) hierarchy.getDimension();
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public RolapAttribute getAttribute() {
        return attribute;
    }

    /**
     * Value that indicates a null parent in a parent-child hierarchy. Typical
     * values are {@code null} and the string {@code "0"}.
     */
    public String getNullParentValue() {
        return nullParentValue;
    }

    public RolapClosure getClosure() {
        return closure;
    }

    HideMemberCondition getHideMemberCondition() {
        return hideMemberCondition;
    }

    public final boolean isUnique() {
        return true; // REVIEW:
    }

    int getOrderByKeyArity() {
        return orderByList.size();
    }

    public RolapAttribute getParentAttribute() {
        return parentAttribute;
    }

    /**
     * Returns whether this level is parent-child.
     */
    public boolean isParentChild() {
        return parentAttribute != null;
    }

    private Property lookupProperty(
        List<RolapProperty> list,
        String propertyName)
    {
        for (Property property : list) {
            if (property.getName().equals(propertyName)) {
                return property;
            }
        }
        return null;
    }

    void initLevel(RolapSchemaLoader schemaLoader) {
        // Initialize, and validate, inherited properties.
        for (RolapLevel level = this;
             level != null;
             level = level.getParentLevel())
        {
            for (final RolapProperty levelProperty
                : level.attribute.getProperties())
            {
                Property existingProperty = lookupProperty(
                    inheritedProperties, levelProperty.getName());
                if (existingProperty == null) {
                    inheritedProperties.add(levelProperty);
                } else if (existingProperty.getType()
                    != levelProperty.getType())
                {
                    throw Util.newError(
                        "Property " + this.getName() + "."
                        + levelProperty.getName() + " overrides a "
                        + "property with the same name but different type");
                }
            }
        }
    }

    public final boolean isAll() {
        return attribute.getLevelType() == org.olap4j.metadata.Level.Type.ALL;
    }

    public boolean areMembersUnique() {
        return (depth == 0) || (depth == 1) && hierarchy.hasAll();
    }

    public RolapProperty[] getProperties() {
        final List<RolapProperty> properties = attribute.getProperties();
        return properties.toArray(new RolapProperty[properties.size()]);
    }

    public Property[] getInheritedProperties() {
        return inheritedProperties.toArray(
            new Property[inheritedProperties.size()]);
    }

    public int getApproxRowCount() {
        if (approxRowCount > 0) {
            return approxRowCount;
        }
        return attribute.getApproxRowCount();
    }

    /**
     * The column(s) that this level is ordered on. Usually the list is
     * similar to the underlying attribute's list.
     */
    public List<RolapSchema.PhysColumn> getOrderByList() {
        return orderByList;
    }

    /**
     * Conditions under which a level's members may be hidden (thereby creating
     * a <dfn>ragged hierarchy</dfn>).
     */
    public enum HideMemberCondition {
        /** A member always appears. */
        Never,

        /** A member doesn't appear if its name is null or empty. */
        IfBlankName,

        /** A member appears unless its name matches its parent's. */
        IfParentsName
    }

    public OlapElement lookupChild(SchemaReader schemaReader, Id.Segment name) {
        return lookupChild(schemaReader, name, MatchType.EXACT);
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, Id.Segment name, MatchType matchType)
    {
        // RolapLevel does not contain members -- members belong to
        // RolapCubeLevel -- so this element has no children.
        return null;
    }
}

// End RolapLevel.java
