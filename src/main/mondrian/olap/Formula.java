/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2000-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 1 March, 2000
*/

package mondrian.olap;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.MdxVisitor;
import mondrian.mdx.MdxVisitorImpl;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

/**
 * A <code>Formula</code> is a clause in an MDX query which defines a Set or a
 * Member.
 */
public class Formula extends QueryPart {

    /** name of set or member */
    private final String[] names;
    /** defining expression */
    private Exp exp;
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
        this(false, names, exp, new MemberProperty[0], null, null);
        createElement(null);
    }

    /**
     * Constructs a formula specifying a member.
     */
    public Formula(String[] names, Exp exp, MemberProperty[] memberProperties) {
        this(true, names, exp, memberProperties, null, null);
    }

    private Formula(
            boolean isMember,
            String[] names,
            Exp exp,
            MemberProperty[] memberProperties,
            Member mdxMember,
            NamedSet mdxSet) {
        this.isMember = isMember;
        this.names = names;
        this.exp = exp;
        this.memberProperties = memberProperties;
        this.mdxMember = mdxMember;
        this.mdxSet = mdxSet;
        assert !(!isMember && mdxMember != null);
        assert !(isMember && mdxSet != null);
    }

    public Object clone() {
        return new Formula(
                isMember,
                names,
                (Exp) exp.clone(),
                MemberProperty.cloneArray(memberProperties),
                mdxMember,
                mdxSet);
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
     *
     * @param validator Validation context to resolve the identifiers in this
     *   formula
     */
    void accept(Validator validator) {
        final boolean scalar = isMember;
        exp = validator.validate(exp, scalar);
        String id = Util.quoteMdxIdentifier(names);
        final Type type = exp.getType();
        if (isMember) {
            if (!TypeUtil.canEvaluate(type)) {
                throw MondrianResource.instance().MdxMemberExpIsSet.ex(exp.toString());
            }
        } else {
            if (!TypeUtil.isSet(type)) {
                throw MondrianResource.instance().MdxSetExpNotSet.ex(id);
            }
        }
        for (int i = 0; i < memberProperties.length; i++) {
            validator.validate(memberProperties[i]);
        }
        // Get the format expression from the property list, or derive it from
        // the formula.
        if (isMember) {
            Exp formatExp = getFormatExp();
            if (formatExp != null) {
                mdxMember.setProperty(Property.FORMAT_EXP.name, formatExp);
            }

            // For each property of the formula, make it a property of the
            // member.
            final List formatPropertyList =
                    Arrays.asList(Property.FORMAT_PROPERTIES);
            for (MemberProperty memberProperty : memberProperties) {
                if (formatPropertyList.contains(memberProperty.getName())) {
                    continue; // we already dealt with format_string props
                }
                final Exp exp = memberProperty.getExp();
                if (exp instanceof Literal) {
                    String value = String.valueOf(((Literal) exp).getValue());
                    mdxMember.setProperty(memberProperty.getName(), value);
                }
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
            if (mdxMember != null) {
                return;
            }
            OlapElement mdxElement = q.getCube();
            final SchemaReader schemaReader = q.getSchemaReader(true);
            for (int i = 0; i < names.length; i++) {
                OlapElement parent = mdxElement;
                mdxElement = schemaReader.getElementChild(parent, names[i]);
                // Don't try to look up the member which the formula is
                // defining. We would only find one if the member is overriding
                // a member at the cube or schema level, and we don't want to
                // change that member's properties.
                if (mdxElement == null || i == names.length - 1) {
                    // this part of the name was not found... define it
                    Level level;
                    Member parentMember = null;
                    if (parent instanceof Member) {
                        parentMember = (Member) parent;
                        level = parentMember.getLevel().getChildLevel();
                    } else {
                        Hierarchy hierarchy = parent.getHierarchy();
                        if (hierarchy == null) {
                            throw MondrianResource.instance().MdxCalculatedHierarchyError.ex(
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


    public void unparse(PrintWriter pw)
    {
        if (isMember) {
            pw.print("member ");
            if (mdxMember != null) {
                pw.print(mdxMember.getUniqueName());
            } else {
                pw.print(Util.quoteMdxIdentifier(names));
            }
        } else {
            pw.print("set ");
            pw.print(Util.quoteMdxIdentifier(names));
        }
        pw.print(" as '");
        exp.unparse(pw);
        pw.print("'");
        if (memberProperties != null) {
            for (MemberProperty memberProperty : memberProperties) {
                pw.print(", ");
                memberProperty.unparse(pw);
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
     */
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
            final Type type = exp.getType();
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
        // several names) return that.
        for (String prop : Property.FORMAT_PROPERTIES) {
            Exp formatExp = getMemberProperty(prop);
            if (formatExp != null) {
                return formatExp;
            }
        }

        // Choose a format appropriate to the expression.
        // For now, only do it for decimals.
        final Type type = exp.getType();
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

        if (!mdxMember.isMeasure()) {
            // Don't try to do any format string inference on non-measure
            // calculated members; that can hide the correct formatting
            // from base measures (see TestCalculatedMembers.testFormatString
            // for an example).
            return null;
        }

        // Burrow into the expression. If we find a member, use its format
        // string.
        try {
            exp.accept(
                new MdxVisitorImpl() {
                    public Object visit(MemberExpr memberExpr) {
                        Exp formatExp = (Exp) memberExpr.getMember().
                            getPropertyValue(Property.FORMAT_EXP.name);
                        if (formatExp != null) {
                            throw new FoundOne(formatExp);
                        }
                        return super.visit(memberExpr);
                    }
                }
            );
            return null;
        } catch (FoundOne foundOne) {
            return foundOne.exp;
        }
    }

    public void compile() {
        // nothing to do
    }

    /**
     * Accepts a visitor to this Formula.
     * The default implementation dispatches to the
     * {@link MdxVisitor#visit(Formula)} method.
     *
     * @param visitor Visitor
     */
    public Object accept(MdxVisitor visitor) {
        final Object o = visitor.visit(this);

        // visit the expression
        exp.accept(visitor);

        return o;
    }

    private static class FoundOne extends RuntimeException {
        private final Exp exp;

        public FoundOne(Exp exp) {
            super();
            this.exp = exp;
        }
    }
}

// End Formula.java
