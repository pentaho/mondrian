/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2010 Pentaho
// All Rights Reserved.
*/
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
