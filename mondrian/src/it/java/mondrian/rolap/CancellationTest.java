/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/
package mondrian.rolap;

import mondrian.calc.TupleList;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.fun.CrossJoinFunDef;
import mondrian.olap.fun.CrossJoinTest;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.test.FoodMartTestCase;

import static org.mockito.Mockito.*;


public class CancellationTest extends FoodMartTestCase {

    public void testNonEmptyListCancellation() throws MondrianException {
        // tests that cancellation/timeout is checked in
        // CrossJoinFunDef.nonEmptyList
        propSaver.set(propSaver.properties.CheckCancelOrTimeoutInterval, 1);
        CrossJoinFunDefTester crossJoinFunDef =
                new CrossJoinFunDefTester(new CrossJoinTest.NullFunDef());
        Result result =
            executeQuery("select store.[store name].members on 0 from sales");
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

    public void testMutableCrossJoinCancellation() throws MondrianException {
        // tests that cancellation/timeout is checked in
        // CrossJoinFunDef.mutableCrossJoin
        propSaver.set(propSaver.properties.CheckCancelOrTimeoutInterval, 1);

        RolapCube salesCube = (RolapCube) cubeByName(
            getTestContext().getConnection(),
            "Sales");
        SchemaReader salesCubeSchemaReader =
            salesCube.getSchemaReader(
                getTestContext().getConnection().getRole()).withLocus();

        TupleList productMembers =
            productMembersPotScrubbersPotsAndPans(salesCubeSchemaReader);

        String selectGenders = "select Gender.members on 0 from sales";
        Result genders = executeQuery(selectGenders);

        Evaluator gendersEval =
            ((RolapResult) genders).getEvaluator(new int[]{0});
        TupleList genderMembers = new UnaryTupleList();
        for (Position pos : genders.getAxes()[0].getPositions()) {
            genderMembers.add(pos);
        }

        Execution execution =
            spy(new Execution(genders.getQuery().getStatement(), 0));
        TupleList mutableCrossJoinResult =
            mutableCrossJoin(productMembers, genderMembers, execution);

        gendersEval.getQuery().getStatement().start(execution);

        // checkCancelOrTimeout should be called once
        // for each tuple from mutableCrossJoin since phase interval is 1
        // plus once for each productMembers item
        // since it gets through SqlStatement.execute
        int expectedCallsQuantity =
            mutableCrossJoinResult.size() + productMembers.size();
        verify(execution, times(expectedCallsQuantity)).checkCancelOrTimeout();
    }

    private TupleList mutableCrossJoin(
        final TupleList list1, final TupleList list2, final Execution execution)
        {
            return Locus.execute(
                execution, "CancellationTest",
                new Locus.Action<TupleList>() {
                    public TupleList execute() {
                        return CrossJoinFunDef.mutableCrossJoin(list1, list2);
                    }
                });
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
