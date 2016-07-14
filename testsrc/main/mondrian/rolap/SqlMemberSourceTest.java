/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2015-2016 Pentaho Corporation..  All rights reserved.
*/
package mondrian.rolap;

import mondrian.olap.Result;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

import org.apache.log4j.Logger;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SqlMemberSourceTest extends FoodMartTestCase {
    public void testForWrongOrdinalColumnLogging() {
        Logger testLogger = mock(Logger.class);
        Logger oldLogger;

        final List<String> logErrors = new ArrayList<String>();
        doAnswer(
            new Answer() {
            public Object answer(InvocationOnMock invocation) {
                logErrors.add(invocation.getArguments()[0].toString());
                return null;
            }})
        .when(testLogger).error(anyString());

        oldLogger = getLogger(SqlMemberSource.class);
        replaceLogger(SqlMemberSource.class, testLogger);

        String query =
                "with member [Measures].cntr as 'Count(Descendants([Time].CurrentMember, [Time].month))'\n"
                + "select [Measures].cntr on 0, [Time].[1997] on 1 from [Sales]";

        try {
            String oldSchema = getTestContext().getRawSchema();

            String newSchema = oldSchema.replaceAll(
                "<Level name=\"Quarter\"",
                "<Level name=\"Quarter\" ordinalColumn=\"month_of_year\"");

            assertNotSame("Corrupt schema definition", oldSchema, newSchema);

            TestContext newContext = getTestContext().withSchema(newSchema);

            Result originalResult = getTestContext().executeQuery(query);
            assertEquals(
                "Running with original schema should log no errors",
                0,
                logErrors.size());

            Result newResult = newContext.executeQuery(query);
            assertEquals(
                "Running with modified schema should log 1 error",
                1,
                logErrors.size());
        } finally {
            replaceLogger(SqlMemberSource.class, oldLogger);
        }
    }

    private boolean replaceLogger(Class targetClass, Logger newLogger) {
        Field field;
        try {
            field = targetClass.getDeclaredField("LOGGER");
            field.setAccessible(true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(
                field, field.getModifiers() & ~Modifier.FINAL);

            field.set(null, newLogger);
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    private Logger getLogger(Class targetClass) {
        try {
            return (Logger) targetClass.getDeclaredField("LOGGER").get(null);
        } catch (Exception e) {
            return null;
        }
    }
}
// End SqlMemberSourceTest.java
