/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;
import mondrian.resource.MondrianResource;

import java.util.ArrayList;
import java.util.List;

/**
 * Definition of the <code>VisualTotals</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Jan 16, 2006
 */
public class VisualTotalsFunDef extends FunDefBase {
    static final Resolver Resolver = new ReflectiveMultiResolver(
            "VisualTotals",
            "VisualTotals(<Set>[, <Pattern>])",
            "Dynamically totals child members specified in a set using a pattern for the total label in the result set.",
            new String[] {"fxx", "fxxS"},
            VisualTotalsFunDef.class);

    public VisualTotalsFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    protected Exp validateArg(
            Validator validator, Exp[] args, int i, int category) {
        final Exp validatedArg = super.validateArg(validator, args, i, category);
        if (i == 0) {
            // The function signature guarantees that we have a set of members
            // or a set of tuples.
            final SetType setType = (SetType) validatedArg.getType();
            final Type elementType = setType.getElementType();
            if (!(elementType instanceof MemberType)) {
                throw MondrianResource.instance().
                        VisualTotalsAppliedToTuples.ex();
            }
        }
        return validatedArg;
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        final StringCalc stringCalc = call.getArgCount() > 1 ?
                compiler.compileString(call.getArg(1)) :
                null;
        return new CalcImpl(call, listCalc, stringCalc);
    }

    /**
     * Calc implementation of the <code>VisualTotals</code> function.
     */
    private static class CalcImpl extends AbstractListCalc {
        private final ListCalc listCalc;
        private final StringCalc stringCalc;

        public CalcImpl(
                ResolvedFunCall call, ListCalc listCalc, StringCalc stringCalc) {
            super(call, new Calc[] {listCalc, stringCalc});
            this.listCalc = listCalc;
            this.stringCalc = stringCalc;
        }

        public List evaluateList(Evaluator evaluator) {
            final List<Member> list = listCalc.evaluateList(evaluator);
            final List<Member> resultList = new ArrayList<Member>(list);
            final int memberCount = list.size();
            for (int i = memberCount - 1; i >= 0; --i) {
                Member member = list.get(i);
                if (i + 1 < memberCount) {
                    Member nextMember = resultList.get(i + 1);
                    if (nextMember != member &&
                            nextMember.isChildOrEqualTo(member)) {
                        resultList.set(
                            i,
                            createMember(member, i, resultList, evaluator));
                    }
                }
            }
            return resultList;
        }

        private VisualTotalMember createMember(
                Member member,
                int i,
                final List<Member> list,
                Evaluator evaluator)
        {
            final String name;
            if (stringCalc != null) {
                final String namePattern = stringCalc.evaluateString(evaluator);
                name = substitute(namePattern, member.getName());
            } else {
                name = member.getName();
            }
            final List<Member> childMemberList =
                followingDescendants(member, i + 1, list);
            final Exp exp = makeExpr(childMemberList);
            final Validator validator = evaluator.getQuery().createValidator();
            final Exp validatedExp = exp.accept(validator);
            return new VisualTotalMember(member, name, validatedExp);
        }

        private List<Member> followingDescendants(
            Member member, int i, final List<Member> list)
        {
            List<Member> childMemberList = new ArrayList<Member>();
            while (i < list.size()) {
                Member descendant = list.get(i);
                if (descendant == member) {
                    // strict descendants only
                    break;
                }
                if (!descendant.isChildOrEqualTo(member)) {
                    break;
                }
                if (descendant instanceof VisualTotalMember) {
                    // Add the visual total member, but skip over its children.
                    VisualTotalMember visualTotalMember =
                            (VisualTotalMember) descendant;
                    childMemberList.add(visualTotalMember);
                    i = lastChildIndex(visualTotalMember.member, i, list);
                    continue;
                }
                childMemberList.add(descendant);
                ++i;
            }
            return childMemberList;
        }

        private int lastChildIndex(Member member, int start, List list) {
            int i = start;
            while (true) {
                ++i;
                if (i >= list.size()) {
                    break;
                }
                Member descendant = (Member) list.get(i);
                if (descendant == member) {
                    // strict descendants only
                    break;
                }
                if (!descendant.isChildOrEqualTo(member)) {
                    break;
                }
            }
            return i;
        }

        private Exp makeExpr(final List childMemberList) {
            Exp[] memberExprs = new Exp[childMemberList.size()];
            for (int i = 0; i < childMemberList.size(); i++) {
                final Member childMember = (Member) childMemberList.get(i);
                memberExprs[i] = new MemberExpr(childMember);
            }
            return new UnresolvedFunCall(
                    "Aggregate",
                    new Exp[] {
                        new UnresolvedFunCall(
                                "{}",
                                Syntax.Braces,
                                memberExprs)
                    });
        }
    }

    /**
     * Calculated member for <code>VisualTotals</code> function.
     *
     * It corresponds to a real member, and most of its properties are similar.
     * The main differences are:<ul>
     * <li>its name is derived from the VisualTotals pattern, e.g.
     *     "*Subtotal - Dairy" as opposed to "Dairy"
     * <li>its value is a calculation computed by aggregating all of the
     *     members which occur following it in the list</ul></p>
     */
    private static class VisualTotalMember extends RolapMember {
        private final Member member;
        private final Exp exp;

        VisualTotalMember(
                Member member,
                String name,
                final Exp exp) {
            super(
                (RolapMember) member.getParentMember(),
                (RolapLevel) member.getLevel(),
                null, name, MemberType.FORMULA);
            this.member = member;
            this.exp = exp;
        }

        public boolean isCalculated() {
            return true;
        }

        public int getSolveOrder() {
            // high solve order, so it is expanded after other calculations
            return 99;
        }

        public Exp getExpression() {
            return exp;
        }

        public int getOrdinal() {
            throw new UnsupportedOperationException();
        }

        public Member getDataMember() {
            return member;
        }

        public OlapElement lookupChild(SchemaReader schemaReader, String s) {
            throw new UnsupportedOperationException();
        }
        
        public OlapElement lookupChild(
            SchemaReader schemaReader, String s, MatchType matchType) {
            throw new UnsupportedOperationException();
        }

        public String getQualifiedName() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Substitutes a name into a pattern.<p/>
     *
     * Asterisks are replaced with the name,
     * double-asterisks are replaced with a single asterisk.
     * For example,
     * <blockquote><code>substitute("** Subtotal - *", "Dairy")</code></blockquote>
     * returns
     * <blockquote><code>"* Subtotal - Dairy"</code></blockquote>
     *
     * @param namePattern Pattern
     * @param name Name to substitute into pattern
     * @return Substituted pattern
     */
    static String substitute(String namePattern, String name) {
        final StringBuilder buf = new StringBuilder(256);
        final int namePatternLen = namePattern.length();
        int startIndex = 0;

        while (true) {
            int endIndex = namePattern.indexOf('*', startIndex);

            if (endIndex == -1) { // No '*' left
                // append the rest of namePattern from startIndex onwards
                buf.append(namePattern.substring(startIndex));
                break;
            }

            // endIndex now points to the '*'; check for '**'
            ++endIndex;
            if (endIndex < namePatternLen && namePattern.charAt(endIndex) == '*') { // Found '**', replace with '*'
                buf.append(namePattern.substring(startIndex, endIndex)); // Include first '*'
                ++endIndex; // Skip over 2nd '*'
            } else { // Found single '*' - substitute (omitting the '*')
                buf.append(namePattern.substring(startIndex, endIndex - 1)); // Exclude '*'
                buf.append(name);
            }

            startIndex = endIndex;
        }

        return buf.toString();
    }

}

// End VisualTotalsFunDef.java
