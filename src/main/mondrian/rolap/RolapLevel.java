/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.Property;
import mondrian.spi.MemberFormatter;

import org.apache.log4j.Logger;
import org.olap4j.metadata.*;

import java.util.*;

/**
 * <code>RolapLevel</code> implements {@link Level} for a ROLAP database.
 *
 * <p>NOTE: This class must not contain any references to XML (MondrianDef)
 * objects. Put those in {@link mondrian.rolap.RolapSchemaLoader}.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
public class RolapLevel extends LevelBase {

    private static final Logger LOGGER = Logger.getLogger(RolapLevel.class);

    protected RolapAttribute attribute;

    private RolapLevel closedPeerLevel;

    private RolapProperty[] inheritedProperties;

    /** Condition under which members are hidden. */
    private final HideMemberCondition hideMemberCondition;
    private final Map<String, Annotation> annotationMap;

    static final RolapProperty KEY_PROPERTY =
        new RolapProperty(
            Property.KEY.name, null, null, Property.Datatype.TYPE_STRING, null,
            null, true);

    static final RolapProperty NAME_PROPERTY =
        new RolapProperty(
            Property.NAME.name, null, null, Property.Datatype.TYPE_STRING, null,
            null, true);

    static final RolapProperty CAPTION_PROPERTY =
        new RolapProperty(
            Property.CAPTION.name, null, null, Property.Datatype.TYPE_STRING,
            null, null, true);

    // TODO: proper name
    static final RolapProperty ORDINAL_PROPERTY =
        new RolapProperty(
            "$ordinal", null, null, Property.Datatype.TYPE_STRING, null, null,
            true);

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
        HideMemberCondition hideMemberCondition,
        Map<String, Annotation> annotationMap)
    {
        super(hierarchy, name, visible, caption, description, depth);
        this.annotationMap = annotationMap;
        this.attribute = attribute;
        this.hideMemberCondition = hideMemberCondition;

        assert annotationMap != null;
        assert hideMemberCondition != null;
    }

    public org.olap4j.metadata.Level.Type getLevelType() {
        return attribute.levelType;
    }

    public RolapHierarchy getHierarchy() {
        return (RolapHierarchy) hierarchy;
    }

    public Map<String, Annotation> getAnnotationMap() {
        return annotationMap;
    }

    public MemberFormatter getMemberFormatter() {
        return attribute.memberFormatter;
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

    /**
     * Returns whether this level is parent-child.
     */
    public boolean isParentChild() {
        return attribute.parentAttribute != null;
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
        }
        this.inheritedProperties = list.toArray(new RolapProperty[list.size()]);

        if (closure) {
            final RolapDimension dimension =
                ((RolapHierarchy) hierarchy).createClosedPeerDimension(this);
            closedPeerLevel =
                dimension.getRolapHierarchyList().get(0)
                    .getRolapLevelList().get(1);
        }
    }

    public final boolean isAll() {
        return attribute.levelType == org.olap4j.metadata.Level.Type.ALL;
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
        return approxRowCount;
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

    public static RolapLevel lookupLevel(
        RolapLevel[] levels,
        String levelName)
    {
        for (RolapLevel level : levels) {
            if (level.getName().equals(levelName)) {
                return level;
            }
        }
        return null;
    }
}

// End RolapLevel.java
