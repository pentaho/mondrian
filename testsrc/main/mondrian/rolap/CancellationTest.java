/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2015 Pentaho Corporation..  All rights reserved.
*/
package mondrian.rolap;

import mondrian.calc.TupleList;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.fun.CrossJoinFunDef;
import mondrian.olap.fun.CrossJoinTest;
import mondrian.server.Execution;
import mondrian.test.FoodMartTestCase;

import static org.mockito.Mockito.*;


public class CancellationTest extends FoodMartTestCase {

    public void testNonEmptyListCancellation() throws MondrianException {
        // tests that cancellation/timeout is checked in
        // CrossJoinFunDef.nonEmptyList
        propSaver.set(propSaver.properties.CheckCancelOrTimeoutInterval, 1);
        CrossJoinFunDefTester crossJoinFunDef =
                new CrossJoinFunDefTester(new CrossJoinTest.NullFunDef());
        Result result = executeQuery(
            "select store.[store name].members on 0 from sales");
        Evaluator eval = ((RolapResult) result).getEvaluator(new int[]{0});
        TupleList list = new UnaryTupleList();
        for (Position pos : result.getAxes()[0].getPositions()) {
            list.add(pos);
        }
        Execution exec = spy(new Execution(eval.getQuery().getStatement(), 0));
        eval.getQuery().getStatement().start(exec);
        crossJoinFunDef.nonEmptyList(eval, list, null);
        // checkCancelOrTimeout should be called once
        // for each tuple since phase interval is 1
        verify(exec, times(list.size())).checkCancelOrTimeout();
    }

    public class CrossJoinFunDefTester extends CrossJoinFunDef {
        public CrossJoinFunDefTester(FunDef dummyFunDef) {
            super(dummyFunDef);
        }

        public TupleList nonEmptyList(
            Evaluator evaluator,
            TupleList list,
            ResolvedFunCall call)
        {
            return super.nonEmptyList(evaluator, list, call);
        }
    }
}
// End CancellationTest.java
