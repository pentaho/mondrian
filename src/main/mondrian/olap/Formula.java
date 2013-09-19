/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2000-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap;

import mondrian.mdx.*;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapCalculatedMember;

import java.io.PrintWriter;
import java.util.*;

/**
 * A <code>Formula</code> is a clause in an MDX query which defines a Set or a
 * Member.
 */
public class Formula extends QueryPart {

    /** name of set or member */
    private final Id id;
    /** defining expression */
    private Exp exp;
    // properties/solve order of member
    private final MemberProperty[] memberProperties;

    /**
     * <code>true</code> is this is a member,
     * <code>false</code> if it is a set.
     */
    private final boolean isMember;

    private Member mdxMember;
    private NamedSet mdxSet;

    /**
     * Constructs formula specifying a set.
     */
    public Formula(Id id, Exp exp) {
        this(false, id, exp, new MemberProperty[0], null, null);
        createElement(null);
    }

    /**
     * Constructs a formula specifying a member.
     */
    public Formula(
        Id id,
        Exp exp,
        MemberProperty[] memberProperties)
    {
        this(true, id, exp, memberProperties, null, null);
    }

    Formula(
        boolean isMember,
        Id id,
        Exp exp,
        MemberProperty[] memberProperties,
        Member mdxMember,
        NamedSet mdxSet)
    {
        this.isMember = isMember;
        this.id = id;
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
            id,
            exp.clone(),
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
        String id = this.id.toString();
        final Type type = exp.getType();
        if (isMember) {
            if (!TypeUtil.canEvaluate(type)) {
                throw MondrianResource.instance().MdxMemberExpIsSet.ex(
                    exp.toString());
            }
        } else {
            if (!TypeUtil.isSet(type)) {
                throw MondrianResource.instance().MdxSetExpNotSet.ex(id);
            }
        }
        for (MemberProperty memberProperty : memberProperties) {
            validator.validate(memberProperty);
        }
        // Get the format expression from the property list, or derive it from
        // the formula.
        if (isMember) {
            Exp formatExp = getFormatExp(validator);
            if (formatExp != null) {
                mdxMember.setProperty(
                    Property.FORMAT_EXP_PARSED.name, formatExp);
                mdxMember.setProperty(
                    Property.FORMAT_EXP.name, Util.unparse(formatExp));
            }

            final List<MemberProperty> memberPropertyList =
                new ArrayList<MemberProperty>(Arrays.asList(memberProperties));

            // put CELL_FORMATTER_SCRIPT_LANGUAGE first, if it exists; we must
            // see it before CELL_FORMATTER_SCRIPT.
            for (int i = 0; i < memberPropertyList.size(); i++) {
                MemberProperty memberProperty = memberPropertyList.get(i);
                if (memberProperty.getName().equals(
                        Property.CELL_FORMATTER_SCRIPT_LANGUAGE.name))
                {
                    memberPropertyList.remove(i);
                    memberPropertyList.add(0, memberProperty);
                }
            }

            // For each property of the formula, make it a property of the
            // member.
            for (MemberProperty memberProperty : memberPropertyList) {
                if (Property.FORMAT_PROPERTIES.contains(
                        memberProperty.getName()))
                {
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
        final List<Id.Segment> segments = id.getSegments();
        if (isMember) {
            if (mdxMember != null) {
                return;
            }
            OlapElement mdxElement = q.getCube();
            final SchemaReader schemaReader = q.getSchemaReader(false);
            for (int i = 0; i < segments.size(); i++) {
                final Id.Segment segment0 = segments.get(i);
                if (!(segment0 instanceof Id.NameSegment)) {
                    throw Util.newError(
                        "Calculated member name must not contain member keys");
                }
                final Id.NameSegment segment = (Id.NameSegment) segment0;
                OlapElement parent = mdxElement;
                mdxElement = null;
                // The last segment of the id is the name of the calculated
                // member so no need to look for a pre-existing child.  This
                // avoids unnecessarily executing SQL and loading children into
                // cache.
                if (i != segments.size() - 1) {
                    mdxElement = schemaReader.getElementChild(parent, segment);
                }

                // Don't try to look up the member which the formula is
                // defining. We would only find one if the member is overriding
                // a member at the cube or schema level, and we don't want to
                // change that member's properties.
                if (mdxElement == null || i == segments.size() - 1) {
                    // this part of the name was not found... define it
                    Level level;
                    Member parentMember = null;
                    if (parent instanceof Member) {
                        parentMember = (Member) parent;
                        level = parentMember.getLevel().getChildLevel();
                        if (level == null) {
                            throw Util.newError(
                                "The '"
                                + segment
                                + "' calculated member cannot be created "
                                + "because its parent is at the lowest level "
                                + "in the "
                                + parentMember.getHierarchy().getUniqueName()
                                + " hierarchy.");
                        }
                    } else {
                        final Hierarchy hierarchy;
                        if (parent instanceof Dimension
                            && MondrianProperties.instance()
                                .SsasCompatibleNaming.get())
                        {
                            Dimension dimension = (Dimension) parent;
                            if (dimension.getHierarchies().length == 1) {
                                hierarchy = dimension.getHierarchies()[0];
                            } else {
                                hierarchy = null;
                            }
                        } else {
                            hierarchy = parent.getHierarchy();
                        }
                        if (hierarchy == null) {
                            throw MondrianResource.instance()
                                .MdxCalculatedHierarchyError.ex(id.toString());
                        }
                        level = hierarchy.getLevels()[0];
                    }
                    if (parentMember != null
                        && parentMember.isCalculated())
                    {
                        throw Util.newError(
                            "The '"
                            + parent
                            + "' calculated member cannot be used as a parent"
                            + " of another calculated member.");
                    }
                    Member mdxMember =
                        level.getHierarchy().createMember(
                            parentMember, level, segment.getName(), this);
                    assert mdxMember != null;
                    mdxElement = mdxMember;
                }
            }
            this.mdxMember = (Member) mdxElement;
        } else {
            // don't need to tell query... it's already in query.formula
            Util.assertTrue(
                segments.size() == 1,
                "set names must not be compound");
            final Id.Segment segment0 = segments.get(0);
            if (!(segment0 instanceof Id.NameSegment)) {
                throw Util.newError(
                    "Calculated member name must not contain member keys");
            }
            // Caption and description are initialized to null, and annotations
            // to the empty map. If named set is defined in the schema, we will
            // give these their true values later.
            mdxSet =
                new SetBase(
                    ((Id.NameSegment) segment0).getName(),
                    null,
                    null,
                    exp,
                    false,
                    Collections.<String, Annotation>emptyMap());
        }
    }

    public Object[] getChildren() {
        Object[] children = new Object[1 + memberProperties.length];
        children[0] = exp;
        System.arraycopy(
            memberProperties, 0,
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
                id.unparse(pw);
            }
        } else {
            pw.print("set ");
            id.unparse(pw);
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

    /**
     * Returns the Identifier of the set or member which is declared by this
     * Formula.
     *
     * @return Identifier
     */
    public Id getIdentifier() {
        return id;
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
        final List<Id.Segment> segments = this.id.getSegments();
        assert Util.last(segments) instanceof Id.NameSegment;
        assert ((Id.NameSegment) Util.last(segments)).name
            .equalsIgnoreCase(oldName);
        segments.set(
            segments.size() - 1,
            new Id.NameSegment(newName));
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
     * @return Solve order, or null if SOLVE_ORDER property is not specified
     *   or is not a number or is not constant
     */
    public Number getSolveOrder() {
        return getIntegerMemberProperty(Property.SOLVE_ORDER.name);
    }

    /**
     * Returns the integer value of a given constant.
     * If the property is not set, or its
     * value is not an integer, or its value is not a constant,
     * returns null.
     *
     * @param name Property name
     * @return Value of the property, or null if the property is not set, or its
     *   value is not an integer, or its value is not a constant.
     */
    private Number getIntegerMemberProperty(String name) {
        Exp exp = getMemberProperty(name);
        if (exp != null && exp.getType() instanceof NumericType) {
            return quickEval(exp);
        }
        return null;
    }

    /**
     * Evaluates a constant numeric expression.
     * @param exp Expression
     * @return Result as a number, or null if the expression is not a constant
     *   or not a number.
     */
    private static Number quickEval(Exp exp) {
        if (exp instanceof Literal) {
            Literal literal = (Literal) exp;
            final Object value = literal.getValue();
            if (value instanceof Number) {
                return (Number) value;
            } else {
                return null;
            }
        }
        if (exp instanceof FunCall) {
            FunCall call = (FunCall) exp;
            if (call.getFunName().equals("-")
                && call.getSyntax() == Syntax.Prefix)
            {
                final Number number = quickEval(call.getArg(0));
                if (number == null) {
                    return null;
                } else if (number instanceof Integer) {
                    return - number.intValue();
                } else {
                    return - number.doubleValue();
                }
            }
        }
        return null;
    }

    /**
     * Deduces a formatting expression for this calculated member. First it
     * looks for properties called "format", "format_string", etc. Then it looks
     * inside the expression, and returns the formatting expression for the
     * first member it finds.
     * @param validator
     */
    private Exp getFormatExp(Validator validator) {
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
            exp.accept(new FormatFinder(validator));
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

        if (visitor.shouldVisitChildren()) {
            // visit the expression
            exp.accept(visitor);
        }
        return o;
    }

    private static class FoundOne extends RuntimeException {
        private final Exp exp;

        public FoundOne(Exp exp) {
            super();
            this.exp = exp;
        }
    }

    /**
     *A visitor for burrowing format information given a member.
     */
    private static class FormatFinder extends MdxVisitorImpl {
        private final Validator validator;

        /**
         *
         * @param validator to resolve unresolved expressions
         */
        public FormatFinder(Validator validator) {
            this.validator = validator;
        }

        public Object visit(MemberExpr memberExpr) {
            Member member = memberExpr.getMember();
            returnFormula(member);
            if (member.isCalculated()
                    && member instanceof RolapCalculatedMember
                    && !hasCyclicReference(memberExpr))
            {
                Formula formula = ((RolapCalculatedMember) member).getFormula();
                formula.accept(validator);
                returnFormula(member);
            }

            return super.visit(memberExpr);
        }

        /**
         *
         * @param expr
         * @return true if there is cyclic reference in expression.
         * This check is required to avoid infinite recursion
         */
        private boolean hasCyclicReference(Exp expr) {
            List<MemberExpr> expList = new ArrayList<MemberExpr>();
            return hasCyclicReference(expr, expList);
        }

        private boolean hasCyclicReference(Exp expr, List<MemberExpr> expList) {
            if (expr instanceof MemberExpr) {
                MemberExpr memberExpr = (MemberExpr) expr;
                if (expList.contains(expr)) {
                    return true;
                }
                expList.add(memberExpr);
                Member member = memberExpr.getMember();
                if (member instanceof RolapCalculatedMember) {
                    RolapCalculatedMember calculatedMember =
                        (RolapCalculatedMember) member;
                    Exp exp1 =
                        calculatedMember.getExpression().accept(validator);
                    return hasCyclicReference(exp1, expList);
                }
            }
            if (expr instanceof FunCall) {
                FunCall funCall = (FunCall) expr;
                Exp[] exps = funCall.getArgs();
                for (int i = 0; i < exps.length; i++) {
                    if (hasCyclicReference(
                            exps[i], cloneForEachBranch(expList)))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        private List<MemberExpr> cloneForEachBranch(List<MemberExpr> expList) {
            ArrayList<MemberExpr> list = new ArrayList<MemberExpr>();
            list.addAll(expList);
            return list;
        }

        private void returnFormula(Member member) {
            if (getFormula(member) != null) {
                throw new FoundOne(getFormula(member));
            }
        }

        private Exp getFormula(Member member) {
            return (Exp)
                member.getPropertyValue(Property.FORMAT_EXP_PARSED.name);
        }
    }
}

// End Formula.java
