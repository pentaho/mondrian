/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
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

    private RolapLevel closedPeerLevel;

    private RolapProperty[] inheritedProperties;

    /** Condition under which members are hidden. */
    private final HideMemberCondition hideMemberCondition;
    private final Map<String, Annotation> annotationMap;

    private final List<RolapSchema.PhysColumn> orderByList;

    static final RolapProperty KEY_PROPERTY =
        new RolapProperty(
            Property.KEY.name, null, null, Property.Datatype.TYPE_STRING, null,
            null, true, null);

    static final RolapProperty NAME_PROPERTY =
        new RolapProperty(
            Property.NAME.name, null, null, Property.Datatype.TYPE_STRING, null,
            null, true, null);

    static final RolapProperty CAPTION_PROPERTY =
        new RolapProperty(
            Property.CAPTION.name, null, null, Property.Datatype.TYPE_STRING,
            null, null, true, null);

    // TODO: proper name
    static final RolapProperty ORDINAL_PROPERTY =
        new RolapProperty(
            "$ordinal", null, null, Property.Datatype.TYPE_STRING, null, null,
            true, null);

    /**
     * Creates a level.
     *
     * @pre parentExp != null || nullParentValue == null
     * @pre properties != null
     * @pre hideMemberCondition != null
     */
    RolapLevel(
        RolapHierarchy hierarchy,
        String name,
        boolean visible,
        String caption,
        String description,
        int depth,
        RolapAttribute attribute,
        List<RolapSchema.PhysColumn> orderByList,
        HideMemberCondition hideMemberCondition,
        Map<String, Annotation> annotationMap)
    {
        super(hierarchy, name, visible, caption, description, depth);
        this.annotationMap = annotationMap;
        this.attribute = attribute;
        this.orderByList = orderByList;
        this.hideMemberCondition = hideMemberCondition;

        assert annotationMap != null;
        assert orderByList != null;
        assert hideMemberCondition != null;
    }

    public org.olap4j.metadata.Level.Type getLevelType() {
        return attribute.getLevelType();
    }

    public RolapHierarchy getHierarchy() {
        return (RolapHierarchy) hierarchy;
    }

    public Map<String, Annotation> getAnnotationMap() {
        return annotationMap;
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
        return (RolapDimension) super.getDimension();
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public RolapAttribute getAttribute() {
        return attribute;
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

    /**
     * Returns whether this level is parent-child.
     */
    public boolean isParentChild() {
        return attribute.getParentAttribute() != null;
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

    // helper for constructor
    void initLevel(
        RolapSchemaLoader schemaLoader,
        boolean closure)
    {
        // Initialize, and validate, inherited properties.
        List<RolapProperty> list = new ArrayList<RolapProperty>();
        for (RolapLevel level = this;
             level != null;
             level = level.getParentLevel())
        {
            for (final RolapProperty levelProperty
                : level.attribute.getProperties())
            {
                Property existingProperty = lookupProperty(
                    list, levelProperty.getName());
                if (existingProperty == null) {
                    list.add(levelProperty);
                } else if (existingProperty.getType()
                    != levelProperty.getType())
                {
                    throw Util.newError(
                        "Property " + this.getName() + "."
                        + levelProperty.getName() + " overrides a "
                        + "property with the same name but different type");
                }
            }
            if (level.isParentChild()) {
                closedPeerLevel =
                    (RolapLevel) level.attribute.getClosure().closedPeerLevel;
            }
        }
        this.inheritedProperties = list.toArray(new RolapProperty[list.size()]);
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
        return inheritedProperties;
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
        if (name instanceof Id.KeySegment) {
            Id.KeySegment keySegment = (Id.KeySegment) name;
            List<Comparable> keyValues = new ArrayList<Comparable>();
            for (Id.NameSegment nameSegment : keySegment.getKeyParts()) {
                final String keyValue = nameSegment.name;
                if (RolapUtil.mdxNullLiteral().equalsIgnoreCase(keyValue)) {
                    keyValues.add(RolapUtil.sqlNullValue);
                } else {
                    keyValues.add(keyValue);
                }
            }
            final List<RolapSchema.PhysColumn> keyExps = attribute.getKeyList();
            if (keyExps.size() != keyValues.size()) {
                throw Util.newError(
                    "Wrong number of values in member key; "
                    + keySegment + " has " + keyValues.size()
                    + " values, whereas level's key has " + keyExps.size()
                    + " columns "
                    + new AbstractList<String>() {
                        public String get(int index) {
                            return keyExps.get(keyExps.size() - 1 - index)
                                .toSql();
                        }

                        public int size() {
                            return keyExps.size();
                        }
                    }
                    + ".");
            }
            return getHierarchy().getMemberReader().getMemberByKey(
                this, keyValues);
        }
        List<Member> levelMembers = schemaReader.getLevelMembers(this, true);
        if (levelMembers.size() > 0) {
            Member parent = levelMembers.get(0).getParentMember();
            return
                RolapUtil.findBestMemberMatch(
                    levelMembers,
                    (RolapMember) parent,
                    this,
                    name,
                    matchType,
                    false);
        }
        return null;
    }

    /**
     * Indicates that level is not ragged and not a parent/child level.
     */
    public boolean isSimple() {
        // most ragged hierarchies are not simple -- see isTooRagged.
        if (isTooRagged()) {
            return false;
        }
        if (isParentChild()) {
            return false;
        }
        // does not work for measures
        if (isMeasure()) {
            return false;
        }
        return true;
    }

    /**
     * Determines whether the specified level is too ragged for native
     * evaluation, which is able to handle one special case of a ragged
     * hierarchy: when the level specified in the query is the leaf level of
     * the hierarchy and HideMemberCondition for the level is IfBlankName.
     * This is true even if higher levels of the hierarchy can be hidden
     * because even in that case the only column that needs to be read is the
     * column that holds the leaf. IfParentsName can't be handled even at the
     * leaf level because in the general case we aren't reading the column
     * that holds the parent. Also, IfBlankName can't be handled for non-leaf
     * levels because we would have to read the column for the next level
     * down for members with blank names.
     *
     * @return true if the specified level is too ragged for native
     *         evaluation.
     */
    private boolean isTooRagged() {
        // Is this the special case of raggedness that native evaluation
        // is able to handle?
        if (getDepth() == getHierarchy().getLevelList().size() - 1) {
            switch (getHideMemberCondition()) {
            case Never:
            case IfBlankName:
                return false;
            default:
                return true;
            }
        }
        // Handle the general case in the traditional way.
        return getHierarchy().isRagged();
    }


    /**
     * Returns true when the level is part of a parent/child hierarchy and has
     * an equivalent closed level.
     */
    boolean hasClosedPeer() {
        return closedPeerLevel != null;
    }

    public RolapLevel getClosedPeer() {
        return closedPeerLevel;
    }
}

// End RolapLevel.java
