/*
 *
 * // This software is subject to the terms of the Eclipse Public License v1.0
 * // Agreement, available at the following URL:
 * // http://www.eclipse.org/legal/epl-v10.html.
 * // You must accept the terms of that agreement to use this software.
 * //
 * // Copyright (C) 2001-2005 Julian Hyde
 * // Copyright (C) 2005-2024 Hitachi Vantara and others
 * // All Rights Reserved.
 * /
 *
 */

package mondrian.olap.fun.sort;

import junit.framework.TestCase;
import mondrian.calc.Calc;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleIterable;
import mondrian.calc.TupleList;
import mondrian.olap.Evaluator;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.Query;
import mondrian.olap.fun.MemberOrderKeyFunDef;
import mondrian.server.Execution;
import mondrian.server.Statement;
import org.apache.commons.collections.comparators.ComparatorChain;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static java.util.Arrays.asList;
import static java.util.stream.IntStream.range;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SorterTest extends TestCase {

  @Mock Evaluator evaluator;
  @Mock Query query;
  @Mock Statement statement;
  @Mock Execution execution;
  @Mock SortKeySpec sortKeySpec1;
  @Mock SortKeySpec sortKeySpec2;
  @Mock TupleIterable tupleIterable;
  @Mock Member member1;
  @Mock Member member2;
  @Mock Hierarchy hierarchy1;
  @Mock Hierarchy hierarchy2;
  @Mock Calc calc1;
  @Mock Calc calc2;
  @Mock ComparatorChain comparatorChain;
  @Captor ArgumentCaptor<TupleComparator> comparatorCaptor;


  public void setUp() throws Exception {
    super.setUp();
    MockitoAnnotations.initMocks( this );
    when( sortKeySpec1.getKey() ).thenReturn( calc1 );
    when( sortKeySpec2.getKey() ).thenReturn( calc2 );
    when( evaluator.getQuery() ).thenReturn( query );
    when( query.getStatement() ).thenReturn( statement );
    when( statement.getCurrentExecution() ).thenReturn( execution );
  }

  // tuple sort paths:
  // +--------------+---------------------+------------------------------+
  // |              |Breaking             |Non-breaking                  |
  // +--------------+---------------------+------------------------------+
  // |OrderByKey    | BreakTupleComparator|HierarchicalTupleKeyComparator|
  // +--------------+---------------------+------------------------------+
  // |Not-OrderByKey| BreakTupleComparator|HierarchicalTupleComparator   |
  // +--------------+---------------------+------------------------------+
  public void testComparatorSelectionBrkOrderByKey() {
    setupSortKeyMocks( true, Sorter.Flag.BASC, Sorter.Flag.BDESC );
    Sorter.applySortSpecToComparator( evaluator, 2, comparatorChain, sortKeySpec1 );
    Sorter.applySortSpecToComparator( evaluator, 2, comparatorChain, sortKeySpec2 );
    verify( comparatorChain ).addComparator( any( TupleExpMemoComparator.BreakTupleComparator.class ), eq( false ) );
    verify( comparatorChain ).addComparator( any( TupleExpMemoComparator.BreakTupleComparator.class ), eq( true ) );
  }

  public void testComparatorSelectionBrkNotOrderByKey() {
    setupSortKeyMocks( false, Sorter.Flag.BASC, Sorter.Flag.BDESC );
    Sorter.applySortSpecToComparator( evaluator, 2, comparatorChain, sortKeySpec1 );
    Sorter.applySortSpecToComparator( evaluator, 2, comparatorChain, sortKeySpec2 );
    verify( comparatorChain ).addComparator( any( TupleExpMemoComparator.BreakTupleComparator.class ), eq( false ) );
    verify( comparatorChain ).addComparator( any( TupleExpMemoComparator.BreakTupleComparator.class ), eq( true ) );
  }

  public void testComparatorSelectionNotBreakingOrderByKey() {
    setupSortKeyMocks( true, Sorter.Flag.ASC, Sorter.Flag.DESC );
    Sorter.applySortSpecToComparator( evaluator, 2, comparatorChain, sortKeySpec1 );
    Sorter.applySortSpecToComparator( evaluator, 2, comparatorChain, sortKeySpec2 );
    verify( comparatorChain ).addComparator( any( HierarchicalTupleKeyComparator.class ), eq( false ) );
    verify( comparatorChain ).addComparator( any( HierarchicalTupleKeyComparator.class ), eq( true ) );
  }

  public void testComparatorSelectionNotBreaking() {
    setupSortKeyMocks( false, Sorter.Flag.ASC, Sorter.Flag.DESC );
    Sorter.applySortSpecToComparator( evaluator, 2, comparatorChain, sortKeySpec1 );
    Sorter.applySortSpecToComparator( evaluator, 2, comparatorChain, sortKeySpec2 );
    verify( comparatorChain, times( 2 ) ).addComparator( comparatorCaptor.capture(), eq( false ) );
    assertTrue( comparatorCaptor.getAllValues().get( 0 ) instanceof HierarchicalTupleComparator );
    assertTrue( comparatorCaptor.getAllValues().get( 1 ) instanceof HierarchicalTupleComparator );
  }


  public void testSortTuplesBreakingByKey() {
    TupleList tupleList = genList();
    setupSortKeyMocks( true, Sorter.Flag.BASC, Sorter.Flag.BDESC );

    TupleList result =
      Sorter.sortTuples( evaluator, tupleIterable, tupleList, asList( sortKeySpec1, sortKeySpec2 ), 2 );
    verifyNoMoreInteractions( tupleIterable ); // list passed in, used instead of iterable
    verify( calc1, atLeastOnce() ).dependsOn( hierarchy1 );
    verify( calc1, atLeastOnce() ).dependsOn( hierarchy2 );
    verify( calc2, atLeastOnce() ).dependsOn( hierarchy2 );
    verify( calc2, atLeastOnce() ).dependsOn( hierarchy1 );
    assertTrue( result.size() == 1000 );
  }

  public void testCancel() {
    setupSortKeyMocks( true, Sorter.Flag.ASC, Sorter.Flag.DESC );
    // pass in a null tupleList, and an iterable.  cancel should be checked while generating the list
    // from the iterable
    Sorter.sortTuples( evaluator, genList(), null, asList( sortKeySpec1, sortKeySpec2 ), 2 );
    verify( execution, atLeastOnce() ).checkCancelOrTimeout();
  }


  private void setupSortKeyMocks( boolean isOrderKeyCalc, Sorter.Flag flag1, Sorter.Flag flag2 ) {
    when( sortKeySpec1.getDirection() ).thenReturn( flag1 );
    when( sortKeySpec2.getDirection() ).thenReturn( flag2 );
    when( calc1.isWrapperFor( MemberOrderKeyFunDef.CalcImpl.class ) ).thenReturn( isOrderKeyCalc );
    when( calc2.isWrapperFor( MemberOrderKeyFunDef.CalcImpl.class ) ).thenReturn( isOrderKeyCalc );
    when( calc1.evaluate( evaluator ) ).thenReturn( 1 );
    when( calc2.evaluate( evaluator ) ).thenReturn( 2 );
    when( calc1.dependsOn( hierarchy1 ) ).thenReturn( true );
    when( calc2.dependsOn( hierarchy2 ) ).thenReturn( true );
    when( member1.getHierarchy() ).thenReturn( hierarchy1 );
    when( member2.getHierarchy() ).thenReturn( hierarchy2 );
  }

  private TupleList genList() {
    TupleList tupleList = TupleCollections.createList( 2 );
    range( 0, 1000 )
      .forEach( i -> tupleList.add( asList( member1, member2 ) ) );
    return tupleList;
  }


}
