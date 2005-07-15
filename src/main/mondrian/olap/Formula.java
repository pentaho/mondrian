/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 1 March, 2000
*/

package mondrian.olap;
import mondrian.olap.type.*;

import java.io.PrintWriter;

/**
 * A <code>Formula</code> is a clause in an MDX query which defines a Set or a
 * Member.
 **/
public class Formula extends QueryPart {

    /** name of set or member **/
    private final String[] names;
    /** defining expression **/
    private ExpBase exp;
    // properties/solve order of member
    private final MemberProperty[] memberProperties;
    /**
     * <code>true</code> is this is a member, <code>false</code> if it is a
     * set.
     */
    private final boolean isMember;

    private Member mdxMember;
    private NamedSet mdxSet;

    /**
     * Constructs formula specifying a set.
     */
    public Formula(String[] names, Exp exp) {
        this(false, names, (ExpBase) exp, new MemberProperty[0]);
        createElement(null);
    }

    /**
     * Constructs a formula specifying a member.
     */
    public Formula(String[] names, Exp exp, MemberProperty[] memberProperties) {
        this(true, names, (ExpBase) exp, memberProperties);
    }

    private Formula(
            boolean isMember,
            String[] names,
            ExpBase exp,
            MemberProperty[] memberProperties) {
        this.isMember = isMember;
        this.names = names;
        this.exp = exp;
        this.memberProperties = memberProperties;
    }

    public Object clone()
    {
        return new Formula(
                isMember,
                names,
                (ExpBase) exp.clone(),
                MemberProperty.cloneArray(memberProperties));
    }

    static Formula[] cloneArray(Formula[] x) {
        Formula[] x2 = new Formula[x.length];
        for (int i = 0; i < x.length; i++) {
            x2[i] = (Formula) x[i].clone();
        }
        return x2;
    }

    /**
     * Resolves identifiers into objects.
     * @param resolver The query which contains this formula.
     */
    void accept(Validator resolver) {
        exp = (ExpBase) resolver.validate(exp);
        String id = Util.quoteMdxIdentifier(names);
        if (isMember) {
            if (!(!exp.isSet() ||
                (exp instanceof FunCall && ((FunCall) exp).isCallToTuple()))) {
                throw Util.getRes().newMdxMemberExpIsSet(id);
            }
        } else {
            if (!exp.isSet()) {
                throw Util.getRes().newMdxSetExpNotSet(id);
            }
        }
        for (int i = 0; i < memberProperties.length; i++) {
            resolver.validate(memberProperties[i]);
        }
        // Get the format expression from the property list, or derive it from
        // the formula.
        if (isMember) {
            Exp formatExp = getFormatExp();
            if (formatExp != null) {
                mdxMember.setProperty(Property.FORMAT_EXP.name, formatExp);
            }
        }
    }

    /**
     * Creates the {@link Member} or {@link NamedSet} object which this formula
     * defines.
     */
    void createElement(Query q) {
        // first resolve the name, bit by bit
        if (isMember) {
            OlapElement mdxElement = q.getCube();
            final SchemaReader schemaReader = q.getSchemaReader(true);
            for (int i = 0; i < names.length; i++) {
                OlapElement parent = mdxElement;
                mdxElement = schemaReader.getElementChild(parent, names[i]);
                if (mdxElement == null) {
                    // this part of the name was not found... define it
                    Level level;
                    Member parentMember = null;
                    if (parent instanceof Member) {
                        parentMember = (Member) parent;
                        level = parentMember.getLevel().getChildLevel();
                    } else {
                        Hierarchy hierarchy = parent.getHierarchy();
                        if (hierarchy == null) {
                            throw Util.getRes().newMdxCalculatedHierarchyError(
                                Util.quoteMdxIdentifier(names));
                        }
                        level = hierarchy.getLevels()[0];
                    }
                    Member mdxMember = level.getHierarchy().createMember(
                        parentMember, level, names[i], this);
                    mdxElement = mdxMember;
                }
            }
            this.mdxMember = (Member) mdxElement;
        } else {
            // don't need to tell query... it's already in query.formula
            Util.assertTrue(
                names.length == 1, "set names must not be compound");
            mdxSet = new SetBase(names[0], exp);
        }
    }

    public Object[] getChildren() {
        Object[] children = new Object[1 + memberProperties.length];
        children[0] = exp;
        System.arraycopy(memberProperties, 0,
            children, 1, memberProperties.length);
        return children;
    }


    public void replaceChild(int ordinal, QueryPart with)
    {
        Util.assertTrue(ordinal == 0);
        exp = (ExpBase) with;
    }

    public void unparse(PrintWriter pw)
    {
        if (isMember) {
            pw.print("member ");
            mdxMember.unparse(pw);
        } else {
            pw.print("set ");
            pw.print(Util.quoteMdxIdentifier(names));
        }
        pw.print(" as '");
        exp.unparse(pw);
        pw.print("'");
        if (memberProperties != null) {
            for (int i = 0; i < memberProperties.length; i++) {
                pw.print(", ");
                memberProperties[i].unparse(pw);
            }
        }
    }

    public boolean isMember() {
        return isMember;
    }

    public NamedSet getNamedSet() {
        return mdxSet;
    }

    String[] getNames() {
        return names;
    }

    /** Returns this formula's name. */
    public String getName() {
        return (isMember)
            ? mdxMember.getName()
            : mdxSet.getName();
    }

    /** Returns this formula's caption. */
    public String getCaption() {
        return (isMember)
            ? mdxMember.getCaption()
            : mdxSet.getName();
    }

    /**
     * Changes the last part of the name to <code>newName</code>. For example,
     * <code>[Abc].[Def].[Ghi]</code> becomes <code>[Abc].[Def].[Xyz]</code>;
     * and the member or set is renamed from <code>Ghi</code> to
     * <code>Xyz</code>.
     **/
    void rename(String newName)
    {
        String oldName = getElement().getName();
        Util.assertTrue(
            this.names[this.names.length - 1].equalsIgnoreCase(oldName));
        this.names[this.names.length - 1] = newName;
        if (isMember) {
            mdxMember.setName(newName);
        } else {
            mdxSet.setName(newName);
        }
    }

    /** Returns the unique name of the member or set. */
    String getUniqueName() {
        return (isMember)
            ? mdxMember.getUniqueName()
            : mdxSet.getUniqueName();
    }

    OlapElement getElement() {
        return (isMember)
            ? (OlapElement) mdxMember
            : (OlapElement) mdxSet;
    }

    public Exp getExpression() {
        return exp;
    }

    private Exp getMemberProperty(String name) {
        return MemberProperty.get(memberProperties, name);
    }

    /**
     * Returns the Member. (Not valid if this formula defines a set.)
     *
     * @pre isMember()
     * @post return != null
     */
    public Member getMdxMember() {
        return mdxMember;
    }

    /**
     * Returns the solve order. (Not valid if this formula defines a set.)
     *
     * @pre isMember()
     * @post return != null
     */
    public int getSolveOrder() {
        Exp exp = getMemberProperty(Property.SOLVE_ORDER.name);
        if (exp != null) {
            final Type type = exp.getTypeX();
            if (type instanceof NumericType) {
                return ((Literal) exp).getIntValue();
            }
        }
        return 0;
    }

    /**
     * Deduces a formatting expression for this calculated member. First it
     * looks for properties called "format", "format_string", etc. Then it looks
     * inside the expression, and returns the formatting expression for the
     * first member it finds.
     */
    private Exp getFormatExp() {
        // If they have specified a format string (which they can do under
        // several names) reutrn that.
        for (int i = 0; i < Property.FORMAT_PROPERTIES.length; i++) {
            Exp formatExp = getMemberProperty(Property.FORMAT_PROPERTIES[i]);
            if (formatExp != null) {
                return formatExp;
            }
        }
        // Choose a format appropriate to the expression.
        // For now, only do it for integers.
        final Type type = exp.getTypeX();
        if (type instanceof DecimalType) {
            int scale = ((DecimalType) type).getScale();
            String formatString = "#,##0";
            if (scale > 0) {
                formatString = formatString + ".";
                while (scale-- > 0) {
                    formatString = formatString + "0";
                }
            }
            return Literal.createString(formatString);
        }
        // Burrow into the expression. If we find a member, use its format
        // string.
        // TODO: Obsolete this code.
        Walker walker = new Walker(exp);
        while (walker.hasMoreElements()) {
            final Object o = walker.nextElement();
            if (o instanceof Member) {
                Exp formatExp = (Exp) ((Member) o).getPropertyValue(
                    Property.FORMAT_EXP.name);
                if (formatExp != null) {
                    return formatExp;
                }
            }
        }
        return null;
    }
}

// End Formula.java
