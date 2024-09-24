/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2017 Hitachi Vantara..  All rights reserved.
*/
package mondrian.rolap.agg;

import mondrian.rolap.CellKey;
import mondrian.util.Pair;

import junit.framework.TestCase;

import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.util.Arrays.asList;
import static mondrian.util.Pair.of;

/**
 * This is a base class for two heirs. It provides several template methods
 * for testing
 * @author Andrey Khayrutdinov
 */
abstract class DenseSegmentBodyTestBase<T extends AbstractSegmentBody, V>
  extends TestCase
{

  final V nonNull = createNonNullValue();
  final V nullValue = createNullValue();

  public void testGetObject_NonNull() {
    T body = withOutAxes(nonNull);
    assertEquals(nonNull, body.getObject(0));
  }

  public void testGetObject_Null() {
    T body = withOutAxes(nullValue);
    assertNull(body.getObject(0));
  }


  public void testGetSize_NoNulls() {
    T body = withOutAxes(nonNull, nonNull, nonNull);
    assertEquals(body.getSize(), body.getEffectiveSize());
  }

  public void testGetSize_HasNulls() {
    T body = withOutAxes(nonNull, nullValue, nonNull);
    assertEquals(3, body.getSize());
    assertEquals(2, body.getEffectiveSize());
  }

  public void testGetSize_OnlyNulls() {
    T body = withOutAxes(nullValue, nullValue, nullValue);
    assertEquals(3, body.getSize());
    assertEquals(0, body.getEffectiveSize());
  }


  public void testGetValueMap_NoNullCells_NoNullAxes() {
    SortedSet<Comparable> axis1 = new TreeSet<Comparable>(asList(1, 2));
    SortedSet<Comparable> axis2 = new TreeSet<Comparable>(asList(3));
    List<Pair<SortedSet<Comparable>, Boolean>> axes = asList(
        of(axis1, false), of(axis2, false));
    T body = withAxes(axes, nonNull, nonNull, nonNull);
    assertValuesMapIsCorrect(body, 3);
  }

  public void testGetValueMap_NoNullCells_HasNullAxes() {
    SortedSet<Comparable> axis1 = new TreeSet<Comparable>(asList(1, 2));
    SortedSet<Comparable> axis2 = new TreeSet<Comparable>(asList(3));
    List<Pair<SortedSet<Comparable>, Boolean>> axes = asList(
        of(axis1, false), of(axis2, true));
    T body = withAxes(axes, nonNull, nonNull, nonNull, nonNull);
    assertValuesMapIsCorrect(body, 4);
  }

  public void testGetValueMap_HasNullCells_NoNullAxes() {
    SortedSet<Comparable> axis1 = new TreeSet<Comparable>(asList(1, 2));
    SortedSet<Comparable> axis2 = new TreeSet<Comparable>(asList(3));
    List<Pair<SortedSet<Comparable>, Boolean>> axes = asList(
        of(axis1, false), of(axis2, false));
    T body = withAxes(axes, nonNull, nullValue, nonNull, nullValue, nonNull);
    assertValuesMapIsCorrect(body, 3);
  }

  public void testGetValueMap_HasNullCells_HasNullAxes() {
    SortedSet<Comparable> axis1 = new TreeSet<Comparable>(asList(1, 2));
    SortedSet<Comparable> axis2 = new TreeSet<Comparable>(asList(3));
    List<Pair<SortedSet<Comparable>, Boolean>> axes = asList(
        of(axis1, false), of(axis2, true));
    T body = withAxes(
        axes, nonNull, nullValue, nonNull, nullValue, nonNull, nonNull);
    assertValuesMapIsCorrect(body, 4);
  }

  public void testGetValueMap_OnlyNullCells_NoNullAxes() {
    SortedSet<Comparable> axis1 = new TreeSet<Comparable>(asList(1, 2));
    SortedSet<Comparable> axis2 = new TreeSet<Comparable>(asList(3));
    List<Pair<SortedSet<Comparable>, Boolean>> axes = asList(
        of(axis1, false), of(axis2, false));
    T body = withAxes(axes, nullValue, nullValue);
    assertValuesMapIsCorrect(body, 0);
  }

  public void testGetValueMap_OnlyNullCells_HasNullAxes() {
    SortedSet<Comparable> axis1 = new TreeSet<Comparable>(asList(1, 2));
    SortedSet<Comparable> axis2 = new TreeSet<Comparable>(asList(3));
    List<Pair<SortedSet<Comparable>, Boolean>> axes = asList(
        of(axis1, false), of(axis2, true));
    T body = withAxes(axes, nullValue, nullValue);
    assertValuesMapIsCorrect(body, 0);
  }

  private void assertValuesMapIsCorrect(T body, int expectedSize) {
    Map<CellKey, Object> valueMap = body.getValueMap();

    assertEquals(expectedSize, valueMap.size());
    assertEquals(expectedSize, valueMap.keySet().size());
    assertEquals(expectedSize, valueMap.values().size());
    assertEquals(expectedSize, valueMap.entrySet().size());

    int i = 0;
    Iterator<Map.Entry<CellKey, Object>> it = valueMap.entrySet().iterator();
    while (i < expectedSize) {
      assertTrue(Integer.toString(i), it.hasNext());
      assertNotNull(it.next());
      i++;
    }
    assertFalse(it.hasNext());
  }

  abstract V createNullValue();
  abstract V createNonNullValue();
  abstract boolean isNull(V value);

  abstract T createSegmentBody(
      BitSet nullValues, Object array,
      List<Pair<SortedSet<Comparable>, Boolean>> axes);

  T withOutAxes(V... values) {
    return withAxes(
        Collections.<Pair<SortedSet<Comparable>, Boolean>>emptyList(),
        values);
  }

  T withAxes(List<Pair<SortedSet<Comparable>, Boolean>> axes, V... values) {
    BitSet nullValues = new BitSet();
    for (int i = 0; i < values.length; i++) {
      if (isNull(values[i])) {
        nullValues.set(i);
      }
    }
    return createSegmentBody(nullValues, values, axes);
  }
}

// End DenseSegmentBodyTestBase.java