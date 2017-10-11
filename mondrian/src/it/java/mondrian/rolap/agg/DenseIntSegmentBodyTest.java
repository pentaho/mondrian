/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2017 Hitachi Vantara..  All rights reserved.
*/
package mondrian.rolap.agg;

import mondrian.util.Pair;

import java.util.BitSet;
import java.util.List;
import java.util.SortedSet;

/**
 * @author Andrey Khayrutdinov
 */
public class DenseIntSegmentBodyTest extends
  DenseSegmentBodyTestBase<DenseIntSegmentBody, Integer>
{

  @Override
  Integer createNullValue() {
    return 0;
  }

  @Override
  Integer createNonNullValue() {
    return 1;
  }

  @Override
  boolean isNull(Integer value) {
    return (value == null) || (value == 0);
  }

  @Override
  DenseIntSegmentBody createSegmentBody(
      BitSet nullValues,
      Object array,
      List<Pair<SortedSet<Comparable>, Boolean>> axes)
  {
    Object[] integers = (Object[]) array;
    int[] values = new int[integers.length];
    for (int i = 0; i < integers.length; i++) {
      values[i] = (Integer)integers[i];
    }
    return new DenseIntSegmentBody(nullValues, values, axes);
  }
}

// End DenseIntSegmentBodyTest.java