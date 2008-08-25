/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.StringCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.mdx.DimensionExpr;
import mondrian.mdx.HierarchyExpr;
import mondrian.resource.MondrianResource;

import java.util.ArrayList;
import java.util.List;

/**
 * Definition of the <code>StrToSet</code> MDX builtin function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class StrToSetFunDef extends FunDefBase {
    static final ResolverImpl Resolver = new ResolverImpl();

    private StrToSetFunDef(int[] parameterTypes) {
        super("StrToSet", "<Set> StrToSet(<String>[, <Dimension>...])",
                "Constructs a set from a string expression.",
                Syntax.Function, Category.Set, parameterTypes);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final StringCalc stringCalc = compiler.compileString(call.getArg(0));
        SetType type = (SetType) call.getType();
        Type elementType = type.getElementType();
        if (elementType instanceof MemberType) {
            final Hierarchy hierarchy = elementType.getHierarchy();
            return new AbstractListCalc(call, new Calc[] {stringCalc}) {
                public List evaluateList(Evaluator evaluator) {
                    String string = stringCalc.evaluateString(evaluator);
                    return parseMemberList(evaluator, string, hierarchy);
                }
            };
        } else {
            TupleType tupleType = (TupleType) elementType;
            final Hierarchy[] hierarchies =
                new Hierarchy[tupleType.elementTypes.length];
            for (int i = 0; i < tupleType.elementTypes.length; i++) {
                hierarchies[i] = tupleType.elementTypes[i].getHierarchy();
            }
            return new AbstractListCalc(call, new Calc[] {stringCalc}) {
                public List evaluateList(Evaluator evaluator) {
                    String string = stringCalc.evaluateString(evaluator);
                    return parseTupleList(evaluator, string, hierarchies);
                }
            };
        }
    }

    private static List<Member[]> parseTupleList(
        Evaluator evaluator,
        String string,
        Hierarchy[] hierarchies)
    {
        List<Member[]> tupleList = new ArrayList<Member[]>();
        Member[] members = new Member[hierarchies.length];
        int i = 0;
        char c;
        while ((c = string.charAt(i++)) == ' ') {
        }
        if (c != '{') {
            throw fail(string, i, "{");
        }
        while (true) {
            i = parseTuple(evaluator, string, i, members, hierarchies);
            tupleList.add(members.clone());
            while ((c = string.charAt(i++)) == ' ') {
            }
            if (c == ',') {
                // fine
            } else if (c == '}') {
                // we're done
                return tupleList;
            } else {
                throw fail(string, i, ", or }");
            }
        }
    }

    /**
     * Parses a tuple, of the form '(member, member, ...)'.
     * There must be precisely one member for each hierarchy.
     *
     * @param evaluator Evaluator, provides a {@link mondrian.olap.SchemaReader}
     *   and {@link Cube}
     * @param string String to parse
     * @param i Position to start parsing in string
     * @param members Output array of members
     * @param hierarchies Hierarchies of the members
     * @return Position where parsing ended in string
     */
    static int parseTuple(
        Evaluator evaluator,
        String string,
        int i,
        Member[] members,
        Hierarchy[] hierarchies)
    {
        char c;
        while ((c = string.charAt(i++)) == ' ') {
        }
        if (c != '(') {
            throw fail(string, i, "(");
        }
        int j = 0;
        while (true) {
            i = parseMember(evaluator, string, i, members, hierarchies, j);
            while ((c = string.charAt(i++)) == ' ') {
            }
            ++j;
            if (j < hierarchies.length) {
                if (c == ',') {
                    // fine
                } else if (c == ')') {
                        // saw ')' before we saw enough members
                        throw Util.newInternal("too few members");
                } else {
                }
            } else {
                if (c == ')') {
                    break;
                } else {
                    throw Util.newInternal("expected ')");
                }
            }
        }
        return i;
    }

    private static List<Member> parseMemberList(
        Evaluator evaluator,
        String string,
        Hierarchy hierarchy)
    {
        Hierarchy[] hierarchies = new Hierarchy[] {hierarchy};
        List<Member> memberList = new ArrayList<Member>();
        Member[] members = {null};
        int i = 0;
        char c;
        while ((c = string.charAt(i++)) == ' ') {
        }
        if (c != '{') {
            throw fail(string, i, "{");
        }
        while (true) {
            i = parseMember(evaluator, string, i, members, hierarchies, 0);
            memberList.add(members[0]);
            while ((c = string.charAt(i++)) == ' ') {
            }
            if (c == ',') {
                // fine
            } else if (c == '}') {
                // we're done
                return memberList;
            } else {
                throw fail(string, i, ", or }");
            }
        }
    }

    // State values
    private static final int BEFORE_SEG = 0;
    private static final int IN_BRACKET_SEG = 1;
    private static final int AFTER_SEG = 2;
    private static final int IN_SEG = 3;

    static int parseMember(
        Evaluator evaluator,
        String string,
        int i,
        Member[] members,
        Hierarchy[] hierarchies,
        int j)
    {
        int k = string.length();
        List<Id.Segment> nameList = new ArrayList<Id.Segment>();
        int state = BEFORE_SEG;
        int start = 0;
        char c;

        loop:
        while (i < k) {
            switch (state) {
            case BEFORE_SEG:
                c = string.charAt(i);
                switch (c) {
                case '[':
                    ++i;
                    start = i;
                    state = IN_BRACKET_SEG;
                    break;

                case ' ':
                    // Skip whitespace, don't change state.
                    ++i;
                    break;

                case ',':
                case '}':
                    break loop;

                case '.':
                    // TODO: test this, case: ".abc"
                    throw Util.newInternal("unexpected: '.'");

                default:
                    // Carry on reading.
                    state = IN_SEG;
                    start = i;
                    break;
                }
                break;

            case IN_SEG:
                c = string.charAt(i);
                switch (c) {
                case '.':
                    nameList.add(
                        new Id.Segment(
                            string.substring(start, i),
                            Id.Quoting.UNQUOTED));
                    state = BEFORE_SEG;
                    ++i;
                default:
                    ++i;
                }
                break;

            case IN_BRACKET_SEG:
                c = string.charAt(i);
                switch (c) {
                case ']':
                    nameList.add(
                        new Id.Segment(
                            string.substring(start, i),
                            Id.Quoting.QUOTED));
                    ++i;
                    state = AFTER_SEG;
                    break;

                default:
                    // Carry on reading.
                    ++i;
                }
                break;

            case AFTER_SEG:
                c = string.charAt(i);
                switch (c) {
                case ' ':
                    // Skip over any spaces
                    // TODO: test this case: '[foo]  .  [bar]'
                    ++i;
                    break;
                case '.':
                    state = BEFORE_SEG;
                    ++i;
                    break;

                default:
                    // We're not looking at the start of a segment. Parse
                    // the member we've seen so far, then return.
                    break loop;
                }
                break;

            default:
                throw Util.newInternal("unexpected state: " + state);
            }
        }

        // End of member.
        Member member =
            (Member)
                Util.lookupCompound(
                    evaluator.getSchemaReader(),
                    evaluator.getCube(),
                    nameList, true, Category.Member);
        members[j] = member;
        if (member.getHierarchy() != hierarchies[j]) {
            // TODO: better error
            throw Util.newInternal("member is of wrong hierarchy");
        }
        return i;
    }

    private static RuntimeException fail(String string, int i, String expecting) {
        throw Util.newInternal("expected '" + expecting + "' at position " + i + " in '" + string + "'");
    }

    public Exp createCall(Validator validator, Exp[] args) {
        final int argCount = args.length;
        if (argCount <= 1) {
            throw MondrianResource.instance().MdxFuncArgumentsNum.ex(getName());
        }
        for (int i = 1; i < argCount; i++) {
            final Exp arg = args[i];
            if (arg instanceof DimensionExpr) {
                // if arg is a dimension, switch to dimension's default
                // hierarchy
                DimensionExpr dimensionExpr = (DimensionExpr) arg;
                Dimension dimension = dimensionExpr.getDimension();
                args[i] = new HierarchyExpr(dimension.getHierarchy());
            } else if (arg instanceof HierarchyExpr) {
                // nothing
            } else {
                throw MondrianResource.instance().MdxFuncNotHier.ex(
                    i + 1, getName());
            }
        }
        return super.createCall(validator, args);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        switch (args.length) {
        case 1:
            // This is a call to the standard version of StrToSet,
            // which doesn't give us any hints about type.
            return new SetType(null);

        case 2:
        {
            final Type argType = args[1].getType();
            return new SetType(
                new MemberType(
                    argType.getDimension(),
                    argType.getHierarchy(),
                    argType.getLevel(),
                    null));
        }

        default:
        {
            // This is a call to Mondrian's extended version of
            // StrToSet, of the form
            //   StrToSet(s, <Hier1>, ... , <HierN>)
            //
            // The result is a set of tuples
            //  (<Hier1>, ... ,  <HierN>)
            final List<Type> list = new ArrayList<Type>();
            for (int i = 1; i < args.length; i++) {
                Exp arg = args[i];
                final Type argType = arg.getType();
                list.add(
                    new MemberType(
                        argType.getDimension(),
                        argType.getHierarchy(),
                        argType.getLevel(),
                        null));
            }
            final Type[] types = list.toArray(new Type[list.size()]);
            return new SetType(new TupleType(types));
        }
        }
    }

    private static class ResolverImpl extends ResolverBase {
        ResolverImpl() {
            super(
                    "StrToSet",
                    "StrToSet(<String Expression>)",
                    "Constructs a set from a string expression.",
                    Syntax.Function);
        }

        public FunDef resolve(
                Exp[] args, Validator validator, int[] conversionCount) {
            if (args.length < 1) {
                return null;
            }
            Type type = args[0].getType();
            if (!(type instanceof StringType)) {
                return null;
            }
            for (int i = 1; i < args.length; i++) {
                Exp exp = args[i];
                if (!(exp instanceof DimensionExpr)) {
                    return null;
                }
            }
            int[] argTypes = new int[args.length];
            argTypes[0] = Category.String;
            for (int i = 1; i < argTypes.length; i++) {
                argTypes[i] = Category.Hierarchy;
            }
            return new StrToSetFunDef(argTypes);
        }

        public FunDef getFunDef() {
            return new StrToSetFunDef(new int[] {Category.String});
        }
    }
}

// End StrToSetFunDef.java
