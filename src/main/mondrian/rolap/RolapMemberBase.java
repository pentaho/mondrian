/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.Calc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.fun.AggregateFunDef;
import mondrian.olap.fun.VisualTotalsFunDef;
import mondrian.resource.MondrianResource;
import mondrian.server.Locus;
import mondrian.spi.PropertyFormatter;
import mondrian.util.*;

import org.apache.commons.collections.map.Flat3Map;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.util.*;

/**
 * Basic implementation of a member in a {@link RolapCubeHierarchy}.
 *
 * <p>This class aims to be memory-efficient. There are as few fields as
 * possible. We pack several properties into the {@link #flags} field, and
 * we store optional attributes (along with annotations, localized resources and
 * properties) in the larder. Minimizing the number of fields is also why this
 * class does not inherit from {@link OlapElementBase}.</p>
 *
 * <b>Developers, please do not add fields to this
 * class without design review</b>.</p>
 *
 * @author jhyde
 * @since 10 August, 2001
 */
public class RolapMemberBase
    implements RolapMember
{
    protected RolapMember parentMember;
    protected final RolapCubeLevel level;
    private String uniqueName;

    /**
     * For members of a level with an ordinal expression defined, the
     * value of that expression for this member as retrieved via JDBC;
     * otherwise null.
     */
    private Comparable orderKey;

    /**
     * Combines member type and other properties, such as whether the member
     * is the 'all' or 'null' member of its hierarchy and whether it is a
     * measure or is calculated, into an integer field.
     *
     * <p>The fields are:<ul>
     * <li>bits 0, 1, 2 ({@link MemberBase#FLAG_TYPE_MASK}) are member type;
     * <li>bit 3 ({@link MemberBase#FLAG_HIDDEN}) is set if the member is
     *     hidden;
     * <li>bit 4 ({@link MemberBase#FLAG_ALL}) is set if this is the all member
     *     of its hierarchy;
     * <li>bit 5 ({@link MemberBase#FLAG_NULL}) is set if this is the null
     *     member of its hierarchy;
     * <li>bit 6 ({@link MemberBase#FLAG_CALCULATED}) is set if this is a
     *     calculated member.
     * <li>bit 7 ({@link MemberBase#FLAG_MEASURE}) is set if this is a measure.
     * <li>bits 8 and 9 support {@link #isParentChildLeaf()}.</li>
     * <li>bits 10 and 11 support {@link #containsAggregateFunction()}.</li>
     * </ul>
     *
     * NOTE: jhyde, 2007/8/10. It is necessary to cache whether the member is
     * 'all', 'calculated' or 'null' in the member's state, because these
     * properties are used so often. If we used a virtual method call - say we
     * made each subclass implement 'boolean isNull()' - it would be slower.
     * We use one flags field rather than 4 boolean fields to save space.
     */
    protected int flags;

    // Leaf takes bits 8 and 9. Bit 8 is whether known. Bit 9 is yes or no.
    private static final int LEAF_MASK = (1 << 8) | (1 << 9);
    private static final int LEAF_YES = (1 << 8) | (1 << 9);
    private static final int LEAF_NO = (1 << 8);

    // Contains aggregate function takes bits 10 and 11. Bit 10 is whether
    // known. Bit 11 is yes or no.
    private static final int AGG_FUN_MASK = (1 << 10) | (1 << 11);
    private static final int AGG_FUN_YES = (1 << 10) | (1 << 11);
    private static final int AGG_FUN_NO = (1 << 10);

    private static final Logger LOGGER = Logger.getLogger(RolapMember.class);
    private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    /** Ordinal of the member within the hierarchy. Some member readers do not
     * use this property; in which case, they should leave it as its default,
     * -1. */
    private int ordinal;
    private final Comparable key;

    /**
     * Maps property name to property value.
     *
     * <p> We expect there to be a lot of members, but few of them will
     * have properties. So to reduce memory usage, when empty, this is set to
     * an immutable empty set.
     */
    protected Larder larder;

    /**
     * Creates a RolapMemberBase.
     *
     * <p>Larder must contain name (if different from that derived from the
     * key), caption (if different from that derived from the name),
     * property values, and annotations. (Only calculated members defined in the
     * schema have annotations.)</p>
     *
     * @param parentMember Parent member
     * @param level Level this member belongs to
     * @param key Key to this member in the underlying RDBMS, per
     *   {@link RolapMember.Key}
     * @param memberType Type of member
     * @param uniqueName Unique name of this member
     * @param larder Larder
     */
    public RolapMemberBase(
        RolapMember parentMember,
        RolapCubeLevel level,
        Comparable key,
        MemberType memberType,
        String uniqueName,
        Larder larder)
    {
        assert Key.isValid(key, level, memberType)
            : "invalid key " + key + " for level " + level;
        assert larder != null;
        this.parentMember = parentMember;
        this.level = level;
        this.flags = memberType.ordinal()
            | (memberType == MemberType.ALL ? MemberBase.FLAG_ALL : 0)
            | (memberType == MemberType.NULL ? MemberBase.FLAG_NULL : 0)
            | (computeCalculated(memberType) ? MemberBase.FLAG_CALCULATED : 0)
            | (level.isMeasure() ? MemberBase.FLAG_MEASURE : 0);
        this.key = key;
        this.ordinal = -1;
        this.larder = larder;
        this.uniqueName = uniqueName;
    }

    public String getQualifiedName() {
        return MondrianResource.instance().MdxMemberName.str(getUniqueName());
    }

    public final String getUniqueName() {
        return uniqueName;
    }

    public String getCaption() {
        // if there is a member formatter for the members level,
        //  we will call this interface to provide the display string
        mondrian.spi.MemberFormatter mf = getLevel().getMemberFormatter();
        if (mf != null) {
            return mf.formatMember(this);
        }
        return Larders.getCaption(this, getLarder());
    }

    /**
     * Sets a member's parent.
     *
     * <p>Can screw up the caching structure. Only to be called by
     * {@link mondrian.olap.CacheControl#createMoveCommand}.
     *
     * <p>New parent must be in same level as old parent.
     *
     * @param parentMember New parent member
     *
     * @see #getParentMember()
     * @see #getParentUniqueName()
     */
    void setParentMember(RolapMember parentMember) {
        final RolapMember previousParentMember = getParentMember();
        if (previousParentMember.getLevel() != parentMember.getLevel()) {
            throw new IllegalArgumentException(
                "new parent belongs to different level than old");
        }
        this.parentMember = parentMember;
    }

    public String getParentUniqueName() {
        return parentMember == null
            ? null
            : parentMember.getUniqueName();
    }

    public MemberType getMemberType() {
        return MemberBase.MEMBER_TYPE_VALUES[flags & MemberBase.FLAG_TYPE_MASK];
    }

    public String getDescription() {
        return Larders.getDescription(getLarder());
    }

    public boolean isMeasure() {
        return (flags & MemberBase.FLAG_MEASURE) != 0;
    }

    public boolean isAll() {
        return (flags & MemberBase.FLAG_ALL) != 0;
    }

    public boolean isNull() {
        return (flags & MemberBase.FLAG_NULL) != 0;
    }

    public boolean isCalculated() {
        return (flags & MemberBase.FLAG_CALCULATED) != 0;
    }

    public boolean isEvaluated() {
        // should just call isCalculated(), but called in tight loops
        // and too many subclass implementations for jit to inline properly?
        return (flags & MemberBase.FLAG_CALCULATED) != 0;
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader,
        Id.Segment childName,
        MatchType matchType)
    {
        return schemaReader.lookupMemberChildByName(
            this, childName, matchType);
    }

    // implement Member
    public boolean isChildOrEqualTo(Member member) {
        // REVIEW: Using uniqueName to calculate ancestry seems inefficient,
        //   because we can't afford to store every member's unique name, so
        //   we want to compute it on the fly
        assert !Bug.BugSegregateRolapCubeMemberFixed;
        return (member != null) && isChildOrEqualTo(member.getUniqueName());
    }

    /**
     * Returns whether this <code>Member</code>'s unique name is equal to, a
     * child of, or a descendant of a member whose unique name is
     * <code>uniqueName</code>.
     */
    public boolean isChildOrEqualTo(String uniqueName) {
        if (uniqueName == null) {
            return false;
        }

        return isChildOrEqualTo(this, uniqueName);
    }

    private static boolean isChildOrEqualTo(
        RolapMember member,
        String uniqueName)
    {
        while (true) {
            String thisUniqueName = member.getUniqueName();
            if (thisUniqueName.equals(uniqueName)) {
                // found a match
                return true;
            }
            // try candidate's parentMember
            member = member.getParentMember();
            if (member == null) {
                // have reached root
                return false;
            }
        }
    }

    /**
     * Computes the value to be returned by {@link #isCalculated()}, so it can
     * be cached in a variable.
     *
     * @param memberType Member type
     * @return Whether this member is calculated
     */
    protected boolean computeCalculated(final MemberType memberType) {
        // If the member is not created from the "with member ..." MDX, the
        // calculated will be null. But it may be still a calculated measure
        // stored in the cube.
        return isCalculatedInQuery() || memberType == MemberType.FORMULA;
    }

    public int getSolveOrder() {
        return -1;
    }

    /**
     * Returns the expression by which this member is calculated. The expression
     * is not null if and only if the member is not calculated.
     *
     * @see #isCalculated()
     */
    public Exp getExpression() {
        return null;
    }

    // implement Member
    public List<Member> getAncestorMembers() {
        final SchemaReader schemaReader =
            getDimension().getSchema().getSchemaReader();
        final ArrayList<Member> ancestorList = new ArrayList<Member>();
        schemaReader.getMemberAncestors(this, ancestorList);
        return ancestorList;
    }

    public RolapMember getDataMember() {
        return null;
    }

    protected Property lookupProperty(String propertyName, boolean matchCase) {
        Property prop = Property.lookup(propertyName, matchCase);
        if (prop == null && getLevel().getProperties() != null) {
            for (Property matchProp : getLevel().getProperties()) {
                if (matchCase) {
                    if (matchProp.getName().equals(propertyName)) {
                      return matchProp;
                    }
                } else {
                    if (matchProp.getName().equalsIgnoreCase(propertyName)) {
                      return matchProp;
                    }
                }
            }
        }
        return prop;
    }

    public final Object getPropertyValue(String propertyName) {
        return getPropertyValue(propertyName, true);
    }

    public final Object getPropertyValue(String propertyName, boolean matchCase)
    {
        Property property = lookupProperty(propertyName, matchCase);
        if (property == null) {
            return null;
        }
        return getPropertyValue(property);
    }

    public final String getPropertyFormattedValue(String propertyName) {
        final Property property = lookupProperty(propertyName, true);
        if (property == null) {
            return "";
        }
        return getPropertyFormattedValue(property);
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public final RolapCube getCube() {
        return level.cube;
    }

    public RolapCubeDimension getDimension() {
        return level.cubeDimension;
    }

    public boolean isVisible() {
        return true;
    }

    public final RolapCubeLevel getLevel() {
        return level;
    }

    public final RolapCubeHierarchy getHierarchy() {
        return level.cubeHierarchy;
    }

    public final RolapMember getParentMember() {
        return parentMember;
    }

    // Regular members do not have annotations. Measures and calculated members
    // do, so they override this method.
    public Larder getLarder() {
        return larder;
    }

    public String toString() {
        return getUniqueName();
    }

    public int hashCode() {
        return getUniqueName().hashCode();
    }

    public boolean equals(Object o) {
        return o == this
            || o instanceof RolapMemberBase
            && equals((RolapMemberBase) o);
    }

    public boolean equals(OlapElement o) {
        return (o instanceof RolapMemberBase)
            && equals((RolapMemberBase) o);
    }

    private boolean equals(RolapMemberBase that) {
        assert that != null; // public method should have checked
        // Do not use equalsIgnoreCase; unique names should be identical, and
        // hashCode assumes this.
        return this.getUniqueName().equals(that.getUniqueName());
    }

    /** Call this very sparingly. */
    protected void setUniqueName(String string) {
        this.uniqueName = string;
    }

    /** Call this very sparingly. */
    protected String computeUniqueName(Object key) {
        String name = keyToString(key);

        // Drop the '[All Xxxx]' segment in regular members.
        // Keep the '[All Xxxx]' segment in the 'all' member.
        // Keep the '[All Xxxx]' segment in calc members.
        // Drop it in visual-totals and parent-child members (which are flagged
        // as calculated, but do have a data member).
        if (parentMember == null
            || (parentMember.isAll()
            && (!isCalculated()
            || this instanceof VisualTotalsFunDef.VisualTotalMember
            || getDataMember() != null)))
        {
            final RolapCubeLevel level = getLevel();
            final RolapCubeHierarchy hierarchy = level.cubeHierarchy;
            final RolapCubeDimension dimension = hierarchy.getDimension();
            if (dimension.getDimensionType()
                == org.olap4j.metadata.Dimension.Type.MEASURE
                && hierarchy.getName().equals(dimension.getName()))
            {
                // Kludge to ensure that calc members are called
                // [Measures].[Foo] not [Measures].[Measures].[Foo]. We can
                // remove this code when we revisit the scheme to generate
                // member unique names.
                return Util.makeFqName(dimension, name);
            } else {
                if (name.equals(level.getName())) {
                    return
                        Util.makeFqName(
                            Util.makeFqName(
                                hierarchy.getUniqueName(),
                                level.getName()),
                            name);
                } else {
                    return Util.makeFqName(hierarchy, name);
                }
            }
        } else {
            return Util.makeFqName(parentMember, name);
        }
    }

    /** Fast and simple way to derive unique name, if you know parent member is
     * not null. */
    protected static String deriveUniqueName(
        RolapMember parentMember,
        String name)
    {
        // Should call special
        assert parentMember != null && !parentMember.isAll();
        assert name != null;
        return Util.makeFqName(parentMember, name);
    }

    /** More complex way to derive unique name if parent member might be null
     * or the all member.
     *
     * @param parentMember Parent member
     * @param level Level
     * @param name Member name
     * @param calc Whether calculated (pass false if member of a parent-child
     *             hierarchy or a visual total member)
     * @return Unique name of member
     */
    public static String deriveUniqueName(
        RolapMember parentMember,
        RolapCubeLevel level,
        String name,
        boolean calc)
    {
        assert name != null;

        return Util.makeFqName(
            deriveRootUniqueName(parentMember, level, name, calc),
            name);
    }

    private static OlapElement deriveRootUniqueName(
        RolapMember parentMember,
        RolapLevel level,
        String name,
        boolean calc)
    {
        // Drop the '[All Xxxx]' segment in regular members.
        // Keep the '[All Xxxx]' segment in the 'all' member.
        // Keep the '[All Xxxx]' segment in calc members.
        // Drop it in visual-totals and parent-child members (which are flagged
        // as calculated, but do have a data member).
        if (parentMember == null || (parentMember.isAll() && !calc)) {
            final RolapHierarchy hierarchy = level.getHierarchy();
            final RolapDimension dimension = hierarchy.getDimension();
            if (dimension.isMeasures()) {
                // Kludge to ensure that calc members are called
                // [Measures].[Foo] not [Measures].[Measures].[Foo]. We can
                // remove this code when we revisit the scheme to generate
                // member unique names.
                return dimension;
            } else if (name.equals(level.getName())) {
                return level;
            } else {
                return hierarchy;
            }
        } else {
            return parentMember;
        }
    }

    public boolean isCalculatedInQuery() {
        return false;
    }

    public String getName() {
        final String name = (String) getPropertyValue(Property.NAME);
        return (name != null)
            ? name
            : keyToString(key);
    }

    public void setName(String name) {
        throw new Error("unsupported");
    }

    public final void setProperty(String propertyName, Object value) {
        Property property = lookupProperty(propertyName, true);
        if (property != null) {
            setProperty(property, value);
        }
    }

    /**
     * Sets a property of this member to a given value.
     *
     * <p>WARNING: Setting system properties such as "$name" may have nasty
     * side-effects.
     */
    public synchronized void setProperty(Property property, Object value) {
        assert property != Property.NAME;
        if (property == Property.MEMBER_ORDINAL) {
            String ordinal = (String) value;
            if (ordinal.startsWith("\"") && ordinal.endsWith("\"")) {
                ordinal = ordinal.substring(1, ordinal.length() - 1);
            }
            final double d = Double.parseDouble(ordinal);
            setOrdinal((int) d);
            if (isMeasure()) {
                setOrderKey((int) d);
            }
        }

        larder = Larders.set(larder, property, value);
    }

    public Object getPropertyValue(Property property) {
        if (property != null) {
            Member parentMember;
            List<RolapMember> list;
            switch (property.ordinal) {
            case Property.NAME_ORDINAL:
                // Do NOT call getName() here. This property is internal,
                // and must fall through to look in the property list.
                break;

            case Property.CAPTION_ORDINAL:
                return getCaption();

            case Property.DESCRIPTION_ORDINAL:
                return getDescription();

            case Property.CONTRIBUTING_CHILDREN_ORDINAL:
                list = new ArrayList<RolapMember>();
                getHierarchy().getMemberReader().getMemberChildren(this, list);
                return list;

            case Property.CATALOG_NAME_ORDINAL:
                // TODO: can't go from member to connection thence to
                // Connection.getCatalogName()
                break;

            case Property.SCHEMA_NAME_ORDINAL:
                return getHierarchy().getDimension().getSchema().getName();

            case Property.CUBE_NAME_ORDINAL:
                // TODO: can't go from member to cube cube yet
                break;

            case Property.DIMENSION_UNIQUE_NAME_ORDINAL:
                return getHierarchy().getDimension().getUniqueName();

            case Property.HIERARCHY_UNIQUE_NAME_ORDINAL:
                return getHierarchy().getUniqueName();

            case Property.LEVEL_UNIQUE_NAME_ORDINAL:
                return getLevel().getUniqueName();

            case Property.LEVEL_NUMBER_ORDINAL:
                return getLevel().getDepth();

            case Property.MEMBER_UNIQUE_NAME_ORDINAL:
                return getUniqueName();

            case Property.MEMBER_NAME_ORDINAL:
                return getName();

            case Property.MEMBER_TYPE_ORDINAL:
                return getMemberType().ordinal();

            case Property.MEMBER_GUID_ORDINAL:
                return null;

            case Property.MEMBER_CAPTION_ORDINAL:
                return getCaption();

            case Property.MEMBER_ORDINAL_ORDINAL:
                return getOrdinal();

            case Property.CHILDREN_CARDINALITY_ORDINAL:
                return Locus.execute(
                    level.getDimension().getSchema().getInternalConnection(),
                    "Member.CHILDREN_CARDINALITY",
                    new Locus.Action<Integer>() {
                        public Integer execute() {
                            if (isAll() && childLevelHasApproxRowCount()) {
                                return getLevel().getChildLevel()
                                    .getApproxRowCount();
                            } else {
                                ArrayList<RolapMember> list =
                                    new ArrayList<RolapMember>();
                                getHierarchy().getMemberReader()
                                    .getMemberChildren(
                                        RolapMemberBase.this, list);
                                return list.size();
                            }
                        }
                    }
                );

            case Property.PARENT_LEVEL_ORDINAL:
                parentMember = getParentMember();
                return parentMember == null
                    ? 0
                    : parentMember.getLevel().getDepth();

            case Property.PARENT_UNIQUE_NAME_ORDINAL:
                parentMember = getParentMember();
                return parentMember == null
                    ? null
                    : parentMember.getUniqueName();

            case Property.PARENT_COUNT_ORDINAL:
                parentMember = getParentMember();
                return parentMember == null ? 0 : 1;

            case Property.VISIBLE_ORDINAL:
                break;

            case Property.MEMBER_KEY_ORDINAL:
            case Property.KEY_ORDINAL:
                return this == this.getHierarchy().getAllMember()
                    ? 0
                    : getKey();

            case Property.SCENARIO_ORDINAL:
                return ScenarioImpl.forMember(this);

            default:
                break;
                // fall through
            }
        }
        return getPropertyFromMap(property);
    }

    /**
     * Returns the value of a property by looking it up in the property map.
     *
     * @param property Property
     * @return Property value
     */
    protected Object getPropertyFromMap(Property property) {
        synchronized (this) {
            return larder.get(property);
        }
    }

    public String getLocalized(LocalizedProperty prop, Locale locale) {
        final List<Larders.Resource> resources = getResources();
        if (resources != null) {
            final String resource =
                Larders.Resource.lookup(prop, locale, resources);
            if (resource != null) {
                return resource;
            }
        }
        return Larders.get(this, getLarder(), prop, locale);
    }

    /**
     * Attempts to retrieve resources defined in the cube namespace
     * first, falling back to the shared resource if not found.
     */
    private List<Larders.Resource> getResources() {
        // first find the resourceMap.  If the map associated
        // with the RolapCubeLevel is null, try
        // the RolapLevel.
        Map<String, List<Larders.Resource>> map =
            level.resourceMap != null ? level.resourceMap
                : level.getRolapLevel().resourceMap;
        if (map != null) {
            List<Larders.Resource> resource =
                map.get(getCube() + "." + uniqueName + ".member");
            return resource != null ? resource
                : map.get(uniqueName + ".member");
        }
        return null;
    }

    protected boolean childLevelHasApproxRowCount() {
        return getLevel().getChildLevel().getApproxRowCount()
            > Integer.MIN_VALUE;
    }

    public Property[] getProperties() {
        return getLevel().getInheritedProperties();
    }

    /**
     * Returns the ordinal of this member within its hierarchy.
     * The default implementation returns -1.
     */
    public int getOrdinal() {
        return ordinal;
    }

    /**
     * Returns the order key of this member among its siblings.
     * The default implementation returns null.
     */
    public Comparable getOrderKey() {
        return orderKey;
    }

    public void setOrdinal(int ordinal) {
        if (this.ordinal == -1) {
            this.ordinal = ordinal;
        }
    }

    public void setOrderKey(Comparable orderKey) {
        if (!level.isMeasure()) {
            assert arity(orderKey) == level.getOrderByKeyArity();
        } else {
            assert arity(orderKey) == 1;
        }
        this.orderKey = orderKey;
    }

    private void resetOrdinal() {
        this.ordinal = -1;
    }

    public Comparable getKey() {
        return this.key;
    }

    // implement RolapMember
    public Comparable getKeyCompact() {
        return this.key;
    }

    public List<Comparable> getKeyAsList() {
        return asList(getKey());
    }

    private static List<Comparable> asList(Comparable key) {
        if (key instanceof List) {
            return (List<Comparable>) key;
        } else {
            return Collections.singletonList(key);
        }
    }

    public Object[] getKeyAsArray() {
        return asArray(getKey());
    }

    private static Object[] asArray(Object key) {
        if (key == null) {
            return EMPTY_OBJECT_ARRAY;
        } else if (key instanceof List) {
            return ((List) key).toArray();
        } else {
            return new Object[] {key};
        }
    }

    private static int arity(Object key) {
        if (key instanceof List) {
            return ((List<Object>) key).size();
        } else {
            return 1;
        }
    }

    /**
     * Compares this member to another {@link RolapMemberBase}.
     *
     * <p>The method first compares on keys; null keys always collate last.
     * If the keys are equal, it compares using unique name.
     *
     * <p>This method does not consider {@link #ordinal} field, because
     * ordinal is only unique within a parent. If you want to compare
     * members which may be at any position in the hierarchy, use
     * {@link mondrian.olap.fun.FunUtil#compareHierarchically}.
     *
     * @return -1 if this is less, 0 if this is the same, 1 if this is greater
     */
    public int compareTo(Object o) {
        RolapMember other = (RolapMember)o;
        if (this.key == null || other.getKey() == null) {
            if (this.key != null) {
                return 1; // not null is greater than null
            }
            if (other.getKey() != null) {
                return -1; // null is less than not null
            }
            // compare by unique name, if both keys are null
            return this.getUniqueName().compareTo(other.getUniqueName());
        }
        // compare by unique name, if one ore both members are null
        if (this.key == RolapUtil.sqlNullValue
            || other.getKey() == RolapUtil.sqlNullValue)
        {
            return this.getUniqueName().compareTo(other.getUniqueName());
        }
        // as both keys are not null, compare by key
        //  String, Double, Integer should be possible
        //  any key object should be "Comparable"
        // anyway - keys should be of the same class
        if (this.key.getClass().equals(other.getKey().getClass())) {
            if (this.key instanceof String) {
                // use a special case sensitive compare name which
                // first compares w/o case, and if 0 compares with case
                return Util.caseSensitiveCompareName(
                    (String) this.key, (String) other.getKey());
            } else {
                return Util.compareKey(this.key, other.getKey());
            }
        }
        // Compare by unique name in case of different key classes.
        // This is possible, if a new calculated member is created
        //  in a dimension with an Integer key. The calculated member
        //  has a key of type String.
        return this.getUniqueName().compareTo(other.getUniqueName());
    }

    public boolean isHidden() {
        final RolapCubeLevel rolapLevel = getLevel();
        switch (rolapLevel.getHideMemberCondition()) {
        case Never:
            return false;

        case IfBlankName:
        {
            // If the key value in the database is null, then we use
            // a special key value whose toString() is "null".
            final String name = getName();
            return name.equals(RolapUtil.mdxNullLiteral())
                || Util.isBlank(name);
        }

        case IfParentsName:
        {
            final Member parentMember = getParentMember();
            if (parentMember == null) {
                return false;
            }
            final String parentName = parentMember.getName();
            final String name = getName();
            return (parentName == null ? "" : parentName).equals(
                name == null ? "" : name);
        }

        default:
            throw Util.badValue(rolapLevel.getHideMemberCondition());
        }
    }

    public int getDepth() {
        if (parentMember != null) {
            return getParentMember().getDepth() + 1;
        } else {
            return getLevel().getDepth();
        }
    }

    public String getPropertyFormattedValue(Property property) {
        Object val = getPropertyValue(property);

        // do we have a formatter ? if yes, use it
        PropertyFormatter pf = property.getFormatter();
        if (pf != null) {
            return pf.formatProperty(this, property.name, val);
        }

        if (val != null && val instanceof Number) {
            // Numbers are a special case. We don't want any
            // scientific notations, so we wrap in a BigDecimal
            // before calling toString. This is cheap to perform here
            // because this method only gets called by the GUI.
            val = new BigDecimal(((Number)val).doubleValue());
        }

        return (val == null)
            ? ""
            : val.toString();
    }

    public boolean isParentChildLeaf() {
        if ((flags & LEAF_MASK) == 0) {
            boolean isParentChildLeaf = getLevel().isParentChild()
                && getDimension().getSchema().getSchemaReader()
                .getMemberChildren(this).size() == 0;
            flags |= isParentChildLeaf ? LEAF_YES : LEAF_NO;
            return isParentChildLeaf;
        }
        return (flags & LEAF_MASK) == LEAF_YES;
    }

    /**
     * Returns a list of member lists where the first member
     * list is the root members while the last member array is the
     * leaf members.
     *
     * <p>If you know that you will need to get all or most of the members of
     * a hierarchy, then calling this which gets all of the hierarchy's
     * members all at once is much faster than getting members one at
     * a time.
     *
     * @param schemaReader Schema reader
     * @param hierarchy  Hierarchy
     * @return List of arrays of members
     */
    public static List<List<Member>> getAllMembers(
        SchemaReader schemaReader,
        Hierarchy hierarchy)
    {
        long start = System.currentTimeMillis();

        try {
            // Getting the members by Level is the fastest way that I could
            // find for getting all of a hierarchy's members.
            List<List<Member>> list = new ArrayList<List<Member>>();
            for (Level level : hierarchy.getLevelList()) {
                List<Member> members =
                    schemaReader.getLevelMembers(level, true);
                if (members != null) {
                    list.add(members);
                }
            }
            return list;
        } finally {
            if (LOGGER.isDebugEnabled()) {
                long end = System.currentTimeMillis();
                LOGGER.debug(
                    "RolapMember.getAllMembers: time=" + (end - start));
            }
        }
    }

    public static int getHierarchyCardinality(
        SchemaReader schemaReader,
        Hierarchy hierarchy)
    {
        int cardinality = 0;
        for (Level level : hierarchy.getLevelList()) {
            cardinality += schemaReader.getLevelCardinality(level, true, true);
        }
        return cardinality;
    }

    /**
     * Sets member ordinal values using a Bottom-up/Top-down algorithm.
     *
     * <p>Gets an array of members for each level and traverses
     * array for the lowest level, setting each member's
     * parent's parent's etc. member's ordinal if not set working back
     * down to the leaf member and then going to the next leaf member
     * and traversing up again.
     *
     * <p>The above algorithm only works for a hierarchy that has all of its
     * leaf members in the same level (that is, a non-ragged hierarchy), which
     * is the norm. After all member ordinal values have been set, traverses
     * the array of members, making sure that all members' ordinals have been
     * set. If one is found that is not set, then one must to a full Top-down
     * setting of the ordinals.
     *
     * <p>The Bottom-up/Top-down algorithm is MUCH faster than the Top-down
     * algorithm.
     *
     * @param schemaReader Schema reader
     * @param seedMember Member
     */
    public static void setOrdinals(
        SchemaReader schemaReader,
        Member seedMember)
    {
        // The following are times for executing different set ordinals
        // algorithms for both the FoodMart Sales cube/Store dimension
        // and a Large Data set with a dimension with about 250,000 members.
        //
        // Times:
        //    Original setOrdinals Top-down
        //       Foodmart: 63ms
        //       Large Data set: 651865ms
        //    Calling getAllMembers before calling original setOrdinals Top-down
        //       Foodmart: 32ms
        //       Large Data set: 73880ms
        //    Bottom-up/Top-down
        //       Foodmart: 17ms
        //       Large Data set: 4241ms
        long start = System.currentTimeMillis();

        try {
            Hierarchy hierarchy = seedMember.getHierarchy();
            int ordinal = hierarchy.hasAll() ? 1 : 0;
            List<List<Member>> levelMembers =
                getAllMembers(schemaReader, hierarchy);
            List<Member> leafMembers =
                levelMembers.get(levelMembers.size() - 1);
            levelMembers = levelMembers.subList(0, levelMembers.size() - 1);

            // Set all ordinals
            for (Member child : leafMembers) {
                ordinal = bottomUpSetParentOrdinals(ordinal, child);
                ordinal = setOrdinal(child, ordinal);
            }

            boolean needsFullTopDown = needsFullTopDown(levelMembers);

            // If we must to a full Top-down, then first reset all ordinal
            // values to -1, and then call the Top-down
            if (needsFullTopDown) {
                for (List<Member> members : levelMembers) {
                    for (Member member : members) {
                        if (member instanceof RolapMemberBase) {
                            ((RolapMemberBase) member).resetOrdinal();
                        }
                    }
                }

                // call full Top-down
                setOrdinalsTopDown(schemaReader, seedMember);
            }
        } finally {
            if (LOGGER.isDebugEnabled()) {
                long end = System.currentTimeMillis();
                LOGGER.debug("RolapMember.setOrdinals: time=" + (end - start));
            }
        }
    }

    /**
     * Returns whether the ordinal assignment algorithm needs to perform
     * the more expensive top-down algorithm. If the hierarchy is 'uneven', not
     * all leaf members are at the same level, then bottom-up setting of
     * ordinals will have missed some.
     *
     * @param levelMembers Array containing the list of members in each level
     * except the leaf level
     * @return whether we need to apply the top-down ordinal assignment
     */
    private static boolean needsFullTopDown(List<List<Member>> levelMembers) {
        for (List<Member> members : levelMembers) {
            for (Member member : members) {
                if (member.getOrdinal() == -1) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Walks up the hierarchy, setting the ordinals of ancestors until it
     * reaches the root or hits an ancestor whose ordinal has already been
     * assigned.
     *
     * <p>Assigns the given ordinal to the ancestor nearest the root which has
     * not been assigned an ordinal, and increments by one for each descendant.
     *
     * @param ordinal Ordinal to assign to deepest ancestor
     * @param child Member whose ancestors ordinals to set
     * @return Ordinal, incremented for each time it was used
     */
    private static int bottomUpSetParentOrdinals(int ordinal, Member child) {
        Member parent = child.getParentMember();
        if ((parent != null) && parent.getOrdinal() == -1) {
            ordinal = bottomUpSetParentOrdinals(ordinal, parent);
            ordinal = setOrdinal(parent, ordinal);
        }
        return ordinal;
    }

    private static int setOrdinal(Member member, int ordinal) {
        if (member instanceof RolapMemberBase) {
            ((RolapMemberBase) member).setOrdinal(ordinal++);
        } else {
            // TODO
            LOGGER.warn(
                "RolapMember.setAllChildren: NOT RolapMember "
                + "member.name=" + member.getName()
                + ", member.class=" + member.getClass().getName()
                + ", ordinal=" + ordinal);
            ordinal++;
        }
        return ordinal;
    }

    /**
     * Sets ordinals of a complete member hierarchy as required by the
     * MEMBER_ORDINAL XMLA element using a depth-first algorithm.
     *
     * <p>For big hierarchies it takes a bunch of time. SQL Server is
     * relatively fast in comparison so it might be storing such
     * information in the DB.
     *
     * @param schemaReader Schema reader
     * @param member Member
     */
    private static void setOrdinalsTopDown(
        SchemaReader schemaReader,
        Member member)
    {
        long start = System.currentTimeMillis();

        try {
            Member parent = schemaReader.getMemberParent(member);

            if (parent == null) {
                // top of the world
                int ordinal = 0;

                List<Member> siblings =
                    schemaReader.getHierarchyRootMembers(member.getHierarchy());

                for (Member sibling : siblings) {
                    ordinal = setAllChildren(ordinal, schemaReader, sibling);
                }

            } else {
                setOrdinalsTopDown(schemaReader, parent);
            }
        } finally {
            if (LOGGER.isDebugEnabled()) {
                long end = System.currentTimeMillis();
                LOGGER.debug(
                    "RolapMember.setOrdinalsTopDown: time=" + (end - start));
            }
        }
    }

    private static int setAllChildren(
        int ordinal,
        SchemaReader schemaReader,
        Member member)
    {
        ordinal = setOrdinal(member, ordinal);

        List<Member> children = schemaReader.getMemberChildren(member);
        for (Member child : children) {
            ordinal = setAllChildren(ordinal, schemaReader, child);
        }

        return ordinal;
    }

    /**
     * Converts a key to a string to be used as part of the member's name
     * and unique name.
     *
     * <p>Usually, it just calls {@link Object#toString}. But if the key is an
     * integer value represented in a floating-point column, we'd prefer the
     * integer value. For example, one member of the
     * <code>[Sales].[Store SQFT]</code> dimension comes out "20319.0" but we'd
     * like it to be "20319".
     */
    protected static String keyToString(Object key) {
        if (key == null || key == RolapUtil.sqlNullValue) {
            return RolapUtil.mdxNullLiteral();
        } else if (key instanceof List) {
            List list = (List) key;
            return keyToString(list.get(list.size() - 1));
        } else if (key instanceof Id.NameSegment) {
            return ((Id.NameSegment) key).name;
        } else if (key instanceof Number) {
            String name = key.toString();
            if (name.endsWith(".0")) {
                name = name.substring(0, name.length() - 2);
            }
            return name;
        } else {
            return key.toString();
        }
    }

    public Map<String, Annotation> getAnnotationMap() {
        return getLarder().getAnnotationMap();
    }

    /**
     * <p>Interface definition for the pluggable factory used to decide
     * which implementation of {@link java.util.Map} to use to store
     * property string/value pairs for member properties.</p>
     *
     * <p>This permits tuning for performance, memory allocation, etcetera.
     * For example, if a member belongs to a level which has 10 member
     * properties a HashMap may be preferred, while if the level has
     * only two member properties a Flat3Map may make more sense.</p>
     */
    public interface PropertyValueMapFactory {
        /**
         * Creates a {@link java.util.Map} to be used for storing
         * property string/value pairs for the specified
         * {@link mondrian.olap.Member}.
         *
         * @param member Member
         * @return the Map instance to store property/value pairs
         */
        Map<Property, Object> create(Member member);
    }

    /**
     * Default {@link RolapMemberBase.PropertyValueMapFactory}
     * implementation, used if
     * {@link mondrian.olap.MondrianProperties#PropertyValueMapFactoryClass}
     * is not set.
     */
    public static final class DefaultPropertyValueMapFactory
        implements PropertyValueMapFactory
    {
        /**
         * {@inheritDoc}
         * <p>This factory creates an
         * {@link org.apache.commons.collections.map.Flat3Map} if
         * it appears that the provided member has less than 3 properties,
         * and a {@link java.util.HashMap} if it appears
         * that it has more than 3.</p>
         *
         * <p>Guessing the number of properties
         * can be tricky since some subclasses of
         * {@link mondrian.olap.Member}</p> have additional properties
         * that aren't explicitly declared.  The most common offenders
         * are the (@link mondrian.olap.Measure} implementations, which
         * often have 4 or more undeclared properties, so if the member
         * is a measure, the factory will create a {@link java.util.HashMap}.
         * </p>
         *
         * @param member {@inheritDoc}
         * @return {@inheritDoc}
         */
        @SuppressWarnings({"unchecked"})
        public Map<Property, Object> create(Member member) {
            assert member != null;
            Property[] props = member.getProperties();
            if ((member instanceof RolapMeasure)
                || (props == null)
                || (props.length > 3))
            {
                return new HashMap<Property, Object>();
            } else {
                return new Flat3Map();
            }
        }
    }

    public boolean containsAggregateFunction() {
        // searching for agg functions is expensive, so cache result
        if ((flags & AGG_FUN_MASK) == 0) {
            boolean containsAggregateFunction =
                foundAggregateFunction(getExpression());
            flags |= containsAggregateFunction ? AGG_FUN_YES : AGG_FUN_NO;
            return containsAggregateFunction;
        }
        return (flags & AGG_FUN_MASK) == AGG_FUN_YES;
    }

    /**
     * Returns whether an expression contains a call to an aggregate
     * function such as "Aggregate" or "Sum".
     *
     * @param exp Expression
     * @return Whether expression contains a call to an aggregate function.
     */
    private static boolean foundAggregateFunction(Exp exp) {
        if (exp instanceof ResolvedFunCall) {
            ResolvedFunCall resolvedFunCall = (ResolvedFunCall) exp;
            if (resolvedFunCall.getFunDef() instanceof AggregateFunDef) {
                return true;
            } else {
                for (Exp argExp : resolvedFunCall.getArgs()) {
                    if (foundAggregateFunction(argExp)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public Calc getCompiledExpression(RolapEvaluatorRoot root) {
        return root.getCompiled(getExpression(), true, null);
    }

    public int getHierarchyOrdinal() {
        return getHierarchy().getOrdinalInCube();
    }

    public void setContextIn(RolapEvaluator evaluator) {
        final RolapMember defaultMember =
            evaluator.root.defaultMembers[getHierarchyOrdinal()];

        // This method does not need to call RolapEvaluator.removeCalcMember.
        // That happens implicitly in setContext.
        evaluator.setContext(defaultMember);
        evaluator.setExpanding(this);
    }
}

// End RolapMemberBase.java
