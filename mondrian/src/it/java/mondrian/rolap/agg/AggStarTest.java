/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import junit.framework.Assert;
import mondrian.recorder.MessageRecorder;
import mondrian.rolap.RolapStar;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.aggmatcher.JdbcSchema;
import mondrian.test.PropertyRestoringTestCase;
import org.mockito.Mockito;

public class AggStarTest extends PropertyRestoringTestCase {

  private AggStar aggStar;
  private RolapStar star;
  private JdbcSchema.Table table;
  private MessageRecorder messageRecorder;
  private static final Long BIG_NUMBER = Integer.MAX_VALUE + 1L;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    star = Mockito.mock(RolapStar.class);
    table = Mockito.mock(JdbcSchema.Table.class);
    messageRecorder = Mockito.mock(MessageRecorder.class);

    Mockito.when(table.getColumnUsages(Mockito.any())).thenCallRealMethod();
    Mockito.when(table.getName()).thenReturn("TestAggTable");
    Mockito.when(table.getTotalColumnSize()).thenReturn(1);
    Mockito.when(table.getNumberOfRows()).thenReturn(BIG_NUMBER);

    aggStar = AggStar.makeAggStar(star, table, messageRecorder, 0L);
    aggStar = Mockito.spy(aggStar);

    propSaver.set(propSaver.properties.ChooseAggregateByVolume, false);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testSizeIntegerOverflow() {
    Assert.assertEquals(BIG_NUMBER.longValue(), aggStar.getSize());
  }

  public void testVolumeIntegerOverflow() {
    propSaver.set(propSaver.properties.ChooseAggregateByVolume, true);
    Assert.assertEquals(BIG_NUMBER.longValue(), aggStar.getSize());
  }
}
