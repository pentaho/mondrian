/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

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