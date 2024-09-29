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
public class DenseDoubleSegmentBodyTest extends
  DenseSegmentBodyTestBase<DenseDoubleSegmentBody, Double>
{

  @Override
  Double createNullValue() {
    return 0d;
  }

  @Override
  Double createNonNullValue() {
    return 1d;
  }

  @Override
  boolean isNull(Double value) {
    return (value == null) || (value == 0);
  }

  @Override
  DenseDoubleSegmentBody createSegmentBody(
      BitSet nullValues,
      Object array,
      List<Pair<SortedSet<Comparable>, Boolean>> axes)
  {
    Object[] doubles = (Object[]) array;
    double[] values = new double[doubles.length];
    for (int i = 0; i < doubles.length; i++) {
      values[i] = (Double)doubles[i];
    }
    return new DenseDoubleSegmentBody(nullValues, values, axes);
  }
}

// End DenseDoubleSegmentBodyTest.java