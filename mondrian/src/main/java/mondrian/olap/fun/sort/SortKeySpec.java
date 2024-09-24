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

package mondrian.olap.fun.sort;

import mondrian.calc.Calc;

public class SortKeySpec {
  private final Calc key;
  private final Sorter.Flag direction;

  public SortKeySpec( Calc key, Sorter.Flag dir ) {
    this.key = key;
    this.direction = dir;
  }

  public Calc getKey() {
    return this.key;
  }

  public Sorter.Flag getDirection() {
    return this.direction;
  }
}
