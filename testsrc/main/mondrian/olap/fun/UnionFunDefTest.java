/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2015 Pentaho Corporation.  All rights reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.TupleList;
import mondrian.calc.impl.DelegatingTupleList;
import mondrian.olap.Member;
import mondrian.rolap.RolapMemberBase;

import junit.framework.TestCase;

import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;


import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;

/**
 * Tests for UnionFunDef
 *
 * @author Yury Bakhmutski
 */
public class UnionFunDefTest extends TestCase {

      /**
     * Test for MONDRIAN-2250 issue.
     * Tuples are gotten from customer attachments.
     */
    public void testUnion() {
            DelegatingTupleList delegatingTupleList1 = new DelegatingTupleList(
                4, new ArrayList<List<Member>>());
            String totalViewingTime = "[Measures].[Total Viewing Time]";
            fillDelegatingTupleLists(delegatingTupleList1, totalViewingTime);

            DelegatingTupleList delegatingTupleList2 = new DelegatingTupleList(
                4, new ArrayList<List<Member>>());
            String averageTimeshift = "[Measures].[Average Timeshift]";
            fillDelegatingTupleLists(delegatingTupleList2, averageTimeshift);

            UnionFunDef unionFunDefMock = Mockito.mock(UnionFunDef.class);
            doCallRealMethod().when(unionFunDefMock).union(
                any(TupleList.class), any(TupleList.class), anyBoolean());

            TupleList tupleList = unionFunDefMock.union(
                delegatingTupleList1, delegatingTupleList2, false);
            assertEquals(40, tupleList.size());
        }

        private void fillDelegatingTupleLists(
            DelegatingTupleList delegatingTupleList, String measure)
        {
            String     consumptionMethod = "[Consumption Method].[PVR]";
            String[] dates = {"[2014-07-25]", "[2014-07-26]", "[2014-07-27]",
                "[2014-07-28]"};
            String consumptionDateCalendar = "[Consumption Date.Calendar]";
            String[] time = {"[00:00]", "[14:00]", "[15:00]", "[16:00]",
                "[23:00]"};
            String consumptionTimeTime = "[Consumption Time.Time]";
            for (int i = 0; i < dates.length; i++) {
                List<Member> tuple = new ArrayList<Member>();
                tuple.add(new MemberForTest(
                    consumptionDateCalendar + "." + dates[i]));
                for (int j = 0; j < time.length; j++) {
                    tuple.add(new MemberForTest(consumptionMethod));
                    tuple.add(new MemberForTest(measure));
                    tuple.add(new MemberForTest(
                        consumptionTimeTime + "." + time[j]));
                    Member[] array = tuple.toArray(new Member[tuple.size()]);
                    delegatingTupleList.addTuple(array);
                    tuple.remove(3);
                    tuple.remove(2);
                    tuple.remove(1);
                }
            }
        }

    private class MemberForTest extends RolapMemberBase {
        private String identifer;

        public MemberForTest(String identifer) {
            this.identifer = identifer;
        }

        @Override
        public String getUniqueName() {
            return identifer;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            throw new AssertionError(
                "HashCode realization supposes equals() should not be called"
                + " with such tuples!");
        }
    };
}

// End UnionFunDefTest.java
