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

import mondrian.olap.type.Type;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.MemberType;
import mondrian.olap.*;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.IntegerCalc;
import mondrian.calc.impl.AbstractTupleCalc;
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.List;

/**
 * Definition of the <code>&lt;Set&gt;.Item(&lt;Index&gt;)</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class SetItemFunDef extends FunDefBase {
    static final SetItemFunDef instance = new SetItemFunDef();

    private SetItemFunDef() {
        super(
                "Item",
                "<Set>.Item(<Index>)",
                "Returns a tuple from the set specified in <Set>. The tuple to be returned is specified by the zero-based position of the tuple in the set in <Index>.",
                "mmxn");
    }

    public Type getResultType(Validator validator, Exp[] args) {
        SetType setType = (SetType) args[0].getType();
        return setType.getElementType();
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
                compiler.compileList(call.getArg(0));
        final IntegerCalc indexCalc =
                compiler.compileInteger(call.getArg(1));
        final Type elementType = ((SetType) listCalc.getType()).getElementType();
        if (elementType instanceof TupleType) {
            final TupleType tupleType = (TupleType) elementType;
            final Member[] nullTuple = makeNullTuple(tupleType);
            return new AbstractTupleCalc(call, new Calc[] {listCalc, indexCalc}) {
                public Member[] evaluateTuple(Evaluator evaluator) {
                    final List list = listCalc.evaluateList(evaluator);
                    assert list != null;
                    final int index = indexCalc.evaluateInteger(evaluator);
                    int listSize = list.size();
                    if (index >= listSize || index < 0) {
                        return nullTuple;
                    } else {
                        return (Member[]) list.get(index);
                    }
                }
            };
        } else {
            final MemberType memberType = (MemberType) elementType;
            final Member nullMember = makeNullMember(memberType);
            return new AbstractMemberCalc(call, new Calc[] {listCalc, indexCalc}) {
                public Member evaluateMember(Evaluator evaluator) {
                    final List list = listCalc.evaluateList(evaluator);
                    assert list != null;
                    final int index = indexCalc.evaluateInteger(evaluator);
                    int listSize = list.size();
                    if (index >= listSize || index < 0) {
                        return nullMember;
                    } else {
                        return (Member) list.get(index);
                    }
                }
            };
        }
    }

    Object makeNullMember(Evaluator evaluator, Exp[] args) {
        final Type elementType = ((SetType) args[0].getType()).getElementType();
        return makeNullMemberOrTuple(elementType);
    }

    Object makeNullMemberOrTuple(final Type elementType) {
        if (elementType instanceof MemberType) {
            MemberType memberType = (MemberType) elementType;
            return makeNullMember(memberType);
        } else if (elementType instanceof TupleType) {
            return makeNullTuple((TupleType) elementType);
        } else {
            throw Util.newInternal("bad type " + elementType);
        }
    }
}

// End SetItemFunDef.java
