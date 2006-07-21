/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;

import mondrian.olap.*;

import org.apache.log4j.Logger;
import java.util.*;

/**
 * A <code>RolapMember</code> is a member of a {@link RolapHierarchy}. There are
 * sub-classes for {@link RolapStoredMeasure}, {@link RolapCalculatedMember}.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
public class RolapMember extends MemberBase {

    private static final Logger LOGGER = Logger.getLogger(RolapMember.class);

    /** 
     * This returns an array of member arrays where the first member
     * array are the root members while the last member array are the
     * leaf members.
     * <p>
     * If you know that you will need to get all or most of the members of
     * a hierarchy, then calling this which gets all of the hierarchy's 
     * members all at once is much faster than getting members one at
     * a time. 
     * 
     * @param schemaReader 
     * @param hierarchy 
     * @return 
     */
    public static Member[][] getAllMembers(SchemaReader schemaReader, 
            Hierarchy hierarchy) {

        long start = System.currentTimeMillis();

        try {
            List list = new ArrayList(500);

            // Getting the members by Level is the fastest way that I could
            // find for getting all of a hierarchy's members.
            Level[] levels = hierarchy.getLevels();
            for (int i = 0; i < levels.length; i++) {
                Level level = levels[i];
                Member[] members = schemaReader.getLevelMembers(level, false);
                if (members != null) {
                    list.add(members);
                }
            }
            return (Member[][]) list.toArray(new Member[list.size()][]);
        } finally {
            if (LOGGER.isDebugEnabled()) {
                long end = System.currentTimeMillis();
                LOGGER.debug("RolapMember.getAllMembers: time=" +(end-start));
            }
        }
    }
    public static int getHierarchyCardinality(SchemaReader schemaReader, 
            Hierarchy hierarchy) {
        Member[][] membersArray = getAllMembers(schemaReader, hierarchy);
        int cardinality = 0;
        for (int i = 0; i < membersArray.length; i++) {
            Member[] members = membersArray[i];
            cardinality += members.length;
        }
        return cardinality;
    }

    /** 
     * This is a Bottom-up/Top-down algorithm for setting member ordinal
     * values. An array of members for each level is gotten and the 
     * array for the lowest level is traversed setting each member's
     * parent's parent's etc. member's ordinal if not set working back
     * down to the leaf member and then going to the next leaf member
     * and traversing up again.
     * <p>
     * The above algorithm only works for hierarchies that have all of its
     * leaf members in the same level, which is the norm. After all
     * member ordinal values have been set, the array of members are
     * traversed making sure that all member's ordinals have been set.
     * If one is found that is not set, then one must to a full Top-down
     * setting of the ordinals.
     * <p>
     * The Bottom-up/Top-down algorithm is MUCH faster than the Top-down
     * algorithm.
     * <p>
     * The following are times for executing different set ordinals
     * algorithms for both the FoodMart Sales cube/Store dimension
     * and a Large Data set with a dimension with about 250,000 members.
     * <p>
     * Times:
     *    Original setOrdinals Top-down
     *       Foodmart: 63ms
     *       Large Data set: 651865ms
     *    Calling getAllMembers before calling original setOrdinals Top-down
     *       Foodmart: 32ms
     *       Large Data set: 73880ms
     *    Bottom-up/Top-down
     *       Foodmart: 17ms
     *       Large Data set: 4241ms
     *
     * 
     * @param schemaReader 
     * @param seedMember 
     */
    public static void setOrdinals(SchemaReader schemaReader, Member seedMember) {
        long start = System.currentTimeMillis();

        try {
            Hierarchy hierarchy = seedMember.getHierarchy();
            //int ordinal = 1;
            int ordinal = hierarchy.hasAll() ? 1 : 0;
            Member[][] membersArray = getAllMembers(schemaReader, hierarchy);
            Member[] leafMembers = membersArray[membersArray.length-1];

            // Set all ordinals, 
            for (int i = 0; i < leafMembers.length; i++) {
                Member child = leafMembers[i];
                ordinal = bottomUpSetParentOrdinals(ordinal, child);
                ordinal = setOrdinal(child, ordinal);
            }

            // Now check to see if all ordinals have been set. If the hierarchy
            // is 'uneven', not all leaf members are at the same level, then
            // the above setting of ordinals will have missed some.
            boolean needsFullTopDown = false;
            for (int i = 0; i < membersArray.length-1; i++) {
                Member[] members = membersArray[i];
                for (int j = 0; j < members.length; j++) {
                    Member member = members[j];
                    if (member.getOrdinal() == -1) {
                        needsFullTopDown = true;
                        break;
                    }
                }
            }
            // If we must to a full Top-down, then first reset all ordinal
            // values to -1, and then call the Top-down
            if (needsFullTopDown) {
                for (int i = 0; i < membersArray.length-1; i++) {
                    Member[] members = membersArray[i];
                    for (int j = 0; j < members.length; j++) {
                        Member member = members[j];
                        if (member instanceof RolapMember) {
                            ((RolapMember) member).resetOrdinal();
                        }
                    }
                }

                // call full Top-down
                setOrdinalsTopDown(schemaReader, seedMember);
            }
        } finally {
            if (LOGGER.isDebugEnabled()) {
                long end = System.currentTimeMillis();
                LOGGER.debug("RolapMember.setOrdinals: time=" +(end-start));
            }
        }
    }
    private static int bottomUpSetParentOrdinals(int ordinal, Member child) {
        Member parent = child.getParentMember();
        if ((parent != null) && parent.getOrdinal() == -1) {
            ordinal = bottomUpSetParentOrdinals(ordinal, parent);
            ordinal = setOrdinal(parent, ordinal);
        }
        return ordinal;
    }

    private static int setOrdinal(Member member, int ordinal) {
        if (member instanceof RolapMember) {
            ((RolapMember) member).setOrdinal(ordinal++);
        } else {
            // TODO
            LOGGER.warn("RolapMember.setAllChildren: NOT RolapMember "+
                "member.name=" +member.getName()+
                ", member.class=" +member.getClass().getName()+
                ", ordinal=" +ordinal
                );
            ordinal++;
        }
        return ordinal;
    }

    /**
     * This does a depth first setting of the complete member hierarchy as
     * required by the MEMBER_ORDINAL XMLA element.
     * For big hierarchies it takes a bunch of time. SQL Server is
     * relatively fast in comparison so it might be storing such
     * information in the DB.
     *
     * @param schemaReader
     * @param member
     */
    public static void setOrdinalsTopDown(SchemaReader schemaReader, Member member) {
        long start = System.currentTimeMillis();

        try {
            Member parent = schemaReader.getMemberParent(member);

            if (parent == null) {
                // top of the world
                int ordinal = 0;

                Member[] siblings = 
                    schemaReader.getHierarchyRootMembers(member.getHierarchy());

                for (int i = 0; i < siblings.length; i++) {
                    Member sibling = siblings[i];
                    ordinal = setAllChildren(ordinal, schemaReader, sibling);
                }

            } else {
                setOrdinalsTopDown(schemaReader, parent);
            }
        } finally {
            if (LOGGER.isDebugEnabled()) {
                long end = System.currentTimeMillis();
                LOGGER.debug("RolapMember.setOrdinalsTopDown: time=" +(end-start));
            }
        }
    }
    private static int setAllChildren(
            int ordinal, SchemaReader schemaReader, Member member) {

        ordinal = setOrdinal(member, ordinal);

        Member[] children = schemaReader.getMemberChildren(member);
        for (int i = 0; i < children.length; i++) {
            Member child = children[i];
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
    private static String keyToString(Object key) {
    	String name = null;
    	if (key == null) {
    		name = RolapUtil.mdxNullLiteral;
    	} else {
    		name = key.toString();
    	}
        if ((key instanceof Number) && name.endsWith(".0")) {
            name = name.substring(0, name.length() - 2);
        }
        return name;
    }

    /** Ordinal of the member within the hierarchy. Some member readers do not
     * use this property; in which case, they should leave it as its default,
     * -1. */
    private int ordinal;
    private final Object key;
    /**
     * Maps property name to property value.
     *
     * <p> We expect there to be a lot of members, but few of them will
     * have properties. So to reduce memory usage, when empty, this is set to
     * an immutable empty set.
     */
    private Map mapPropertyNameToValue;

    /**
     * Creates a RolapMember
     *
     * @param parentMember Parent member
     * @param level Level this member belongs to
     * @param key Key to this member in the underlying RDBMS
     * @param name Name of this member
     * @param flags Flags describing this member (see {@link #flags}
     */
    protected RolapMember(
            Member parentMember,
            RolapLevel level,
            Object key,
            String name,
            int flags) {
        super(parentMember, level, flags);

        this.key = key;
        this.ordinal = -1;
        this.mapPropertyNameToValue = Collections.EMPTY_MAP;

        if (name != null &&
                !(key != null && name.equals(key.toString()))) {
            // Save memory by only saving the name as a property if it's
            // different from the key.
            setProperty(Property.NAME.name, name);
        } else {
            setUniqueName(key);
        }
    }

    RolapMember(Member parentMember, RolapLevel level, Object value) {
        this(parentMember, level, value, null, REGULAR_MEMBER_TYPE);
    }


    protected Logger getLogger() {
        return LOGGER;
    }

    RolapLevel getRolapLevel() {
        return (RolapLevel) level;
    }

    RolapHierarchy getRolapHierarchy() {
        return (RolapHierarchy) getHierarchy();
    }

    public boolean equals(Object o) {
        return (o instanceof RolapMember) &&
                equals((RolapMember) o);
    }

    public boolean equals(OlapElement o) {
        return (o instanceof RolapMember) &&
                equals((RolapMember) o);
    }

    private boolean equals(RolapMember that) {
        assert that != null; // public method should have checked
        return this.getUniqueName().equalsIgnoreCase(that.getUniqueName());
    }

    void makeUniqueName(HierarchyUsage hierarchyUsage) {
        if (parentMember == null && key != null) {
            String n = hierarchyUsage.getName();
            if (n != null) {
                String name = keyToString(key);
                n = Util.quoteMdxIdentifier(n);
                this.uniqueName = Util.makeFqName(n, name);
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("RolapMember.makeUniqueName: uniqueName="
                            +uniqueName);
                }
            }
        }
    }

    private void setUniqueName(Object key) {
        String name = keyToString(key);
        this.uniqueName = (parentMember == null)
            ? Util.makeFqName(getHierarchy(), name)
            : Util.makeFqName(parentMember, name);
    }


    public boolean isCalculatedInQuery() {
        return false;
    }

    public String getName() {
        final String name =
                (String) getPropertyValue(Property.NAME.name);
        return (name != null)
            ? name
            : keyToString(key);
    }

    public void setName(String name) {
        throw new Error("unsupported");
    }

    /**
     * Sets a property of this member to a given value.
     *
     * <p>WARNING: Setting system properties such as "$name" may have nasty
     * side-effects.
     */
    public synchronized void setProperty(String name, Object value) {
        if (name.equals(Property.CAPTION.name)) {
            setCaption((String)value);
            return;
        }

        if (mapPropertyNameToValue.isEmpty()) {
            // the empty map is shared and immutable; create our own
            mapPropertyNameToValue = new HashMap();
        }
        if (name.equals(Property.NAME.name)) {
            if (value == null) {
            	value = RolapUtil.mdxNullLiteral;
            }
        	setUniqueName(value);
        }

        if (name.equals(Property.MEMBER_ORDINAL.name)) {
            String ordinal = (String) value;
            if (ordinal.startsWith("\"") && ordinal.endsWith("\"")) {
                ordinal = ordinal.substring(1, ordinal.length() - 1);
            }
            final double d = Double.parseDouble(ordinal);
            setOrdinal((int) d);
        }

        mapPropertyNameToValue.put(name, value);
    }

    public Object getPropertyValue(String name) {
        Property property = Property.lookup(name);
        if (property != null) {
            Schema schema;
            Member parentMember;
            List list;
            switch (property.ordinal) {
            case Property.NAME_ORDINAL:
                // Do NOT call getName() here. This property is internal,
                // and must fall through to look in the property list.
                break;

            case Property.CAPTION_ORDINAL:
                return getCaption();

            case Property.CONTRIBUTING_CHILDREN_ORDINAL:
                list = new ArrayList();
                getRolapHierarchy().getMemberReader().getMemberChildren(this, list);
                return list;

            case Property.CATALOG_NAME_ORDINAL:
                // TODO: can't go from member to connection thence to
                // Connection.getCatalogName()
                break;

            case Property.SCHEMA_NAME_ORDINAL:
                schema = getHierarchy().getDimension().getSchema();
                return schema.getName();

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
                return new Integer(getLevel().getDepth());

            case Property.MEMBER_UNIQUE_NAME_ORDINAL:
                return getUniqueName();

            case Property.MEMBER_NAME_ORDINAL:
                return getName();

            case Property.MEMBER_TYPE_ORDINAL:
                return new Integer(getMemberType());

            case Property.MEMBER_GUID_ORDINAL:
                return null;

            case Property.MEMBER_CAPTION_ORDINAL:
                return getCaption();

            case Property.MEMBER_ORDINAL_ORDINAL:
                return new Integer(getOrdinal());

            case Property.CHILDREN_CARDINALITY_ORDINAL:
                list = new ArrayList();
                getRolapHierarchy().getMemberReader().getMemberChildren(this, list);
                return new Integer(list.size());

            case Property.PARENT_LEVEL_ORDINAL:
                parentMember = getParentMember();
                return new Integer(
                        parentMember == null ? -1 :
                        parentMember.getLevel().getDepth());

            case Property.PARENT_UNIQUE_NAME_ORDINAL:
                parentMember = getParentMember();
                return parentMember == null ? null :
                        parentMember.getUniqueName();

            case Property.PARENT_COUNT_ORDINAL:
                parentMember = getParentMember();
                return new Integer(parentMember == null ? 0 : 1);

            case Property.DESCRIPTION_ORDINAL:
                return getDescription();

            case Property.VISIBLE_ORDINAL:
                break;

            default:
                break;
                // fall through
            }
        }
        synchronized (this) {
            return mapPropertyNameToValue.get(name);
        }
    }

    public Property[] getProperties() {
        return level.getInheritedProperties();
    }

    String quoteKeyForSql() {
        return ((getRolapLevel().getFlags() & RolapLevel.NUMERIC) != 0)
            ? key.toString()
            : Util.singleQuoteString(key.toString());
    }


    public int getOrdinal() {
        return ordinal;
    }

    // RME : before checkin remove public
    public void setOrdinal(int ordinal) {
        if (this.ordinal == -1) {
            this.ordinal = ordinal;
        }
    }
    private void resetOrdinal() {
        this.ordinal = -1;
    }

    Object getKey() {
        return this.key;
    }

    /**
     * Compares this member to another {@link RolapMember}.
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
        if (this.key != null && other.key == null) {
            return 1; // not null is greater than null
        }
        if (this.key == null && other.key != null) {
            return -1; // null is less than not null
        }
        // compare by unique name, if both keys are null
        if (this.key == null && other.key == null) {
            return this.getUniqueName().compareTo(other.getUniqueName());
        }
        // compare by unique name, if one ore both members are null
        if (this.key == RolapUtil.sqlNullValue ||
            other.key == RolapUtil.sqlNullValue) {
            return this.getUniqueName().compareTo(other.getUniqueName());
        }
        // as both keys are not null, compare by key
        //  String, Double, Integer should be possible
        //  any key object should be "Comparable"
        // anyway - keys should be of the same class
        if (this.key.getClass().equals(other.key.getClass())) {
            if (this.key instanceof String) {
                return Util.compareName((String) this.key, (String) other.key);
            } else {
                return ((Comparable) this.key).compareTo(other.key);
            }
        }
        // Compare by unique name in case of different key classes.
        // This is possible, if a new calculated member is created
        //  in a dimension with an Integer key. The calculated member
        //  has a key of type String.
        return this.getUniqueName().compareTo(other.getUniqueName());
    }

    public boolean isHidden() {
        final RolapLevel rolapLevel = getRolapLevel();
        switch (rolapLevel.getHideMemberCondition().ordinal) {
        case RolapLevel.HideMemberCondition.NeverORDINAL:
            return false;
        case RolapLevel.HideMemberCondition.IfBlankNameORDINAL: {
            // If the key value in the database is null, then we use
            // a special key value whose toString() is "null".
            final String name = getName();
            return name.equals(RolapUtil.mdxNullLiteral) || name.equals("");
        }

        case RolapLevel.HideMemberCondition.IfParentsNameORDINAL: {
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
            throw rolapLevel.getHideMemberCondition().unexpected();
        }
    }

    public int getDepth() {
        return level.getDepth();
    }

    public Object getSqlKey() {
        return key;
    }

    /**
     * Returns the formatted value of the property named
     * <code>propertyName</code>.
     */
    public String getPropertyFormattedValue(String propertyName) {
        // do we have a formatter ? if yes, use it
        Property[] props = getLevel().getProperties();
        Property prop = null;
        for (int i = 0; i < props.length; i++) {
            if (props[i].getName().equals(propertyName)) {
                prop = props[i];
                break;
            }
        }
        PropertyFormatter pf;
        if (prop!=null && (pf = prop.getFormatter()) != null) {
            return pf.formatProperty(this, propertyName,
                getPropertyValue(propertyName));
        }

        Object val = getPropertyValue(propertyName);
        return (val == null)
            ? ""
            : val.toString();
    }

}

// End RolapMember.java
