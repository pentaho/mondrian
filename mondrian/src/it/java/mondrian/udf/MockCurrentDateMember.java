/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package mondrian.udf;

import mondrian.olap.Evaluator;

import java.util.Calendar;
import java.util.Date;

public class MockCurrentDateMember extends CurrentDateMemberExactUdf {
    public MockCurrentDateMember() {
        super();
    }
    @Override
    Date getDate(Evaluator evaluator, Argument[] arguments) {
        Calendar cal = Calendar.getInstance();
        cal.set(1997, 1, 1);
        return new Date(cal.getTimeInMillis());
    }
    @Override
    public String getName() {
        return "MockCurrentDateMember";
    }
}
// End MockCurrentDateMember.java
