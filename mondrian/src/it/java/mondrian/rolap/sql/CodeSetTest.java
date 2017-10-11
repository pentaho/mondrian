/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
 */
package mondrian.rolap.sql;

import mondrian.olap.MondrianException;
import mondrian.spi.impl.PostgreSqlDialect;

import junit.framework.TestCase;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Tatsiana_Kasiankova
 *
 */
public class CodeSetTest extends TestCase {

  private static final String MONDRIAN_ERROR_NO_GENERIC_VARIANT =
      "Mondrian Error:Internal error: View has no 'generic' variant";
  private static final String POSTGRESQL_DIALECT = "postgresql";
  private static final String SQL_CODE_FOR_POSTGRESQL_DIALECT =
      "Code for dialect='postgresql'";
  private static final String POSTGRES_DIALECT = "postgres";
  private static final String SQL_CODE_FOR_POSTGRES_DIALECT =
      "Code for dialect='postgres'";
  private static final String GENERIC_DIALECT = "generic";
  private static final String SQL_CODE_FOR_GENERIC_DIALECT =
      "Code for dialect='generic'";

  private static final String POSTGRESQL_PRODUCT_VERSION = "9.1.14";
  private static final String POSTGRESQL_PRODUCT_NAME = "PostgreSQL";
  private static final String EMPTY_NAME = "";
  private SqlQuery.CodeSet codeSet;

  @Override
  protected void setUp() throws Exception {
  }

  /**
   * ISSUE MONDRIAN-2335 If SqlQuery.CodeSet contains only sql code
   * for dialect="postgres", this code should be chosen.
   * No error should be thrown
   *
   * @throws Exception
   */
  public void testSucces_CodeSetContainsOnlyCodeForPostgresDialect()
    throws Exception
    {
    PostgreSqlDialect postgreSqlDialect = new PostgreSqlDialect(
        mockConnection(
            POSTGRESQL_PRODUCT_NAME,
            POSTGRESQL_PRODUCT_VERSION));
    codeSet = new SqlQuery.CodeSet();
    codeSet.put(POSTGRES_DIALECT, SQL_CODE_FOR_POSTGRES_DIALECT);
    try {
      String chooseQuery = codeSet.chooseQuery(postgreSqlDialect);
      assertEquals(SQL_CODE_FOR_POSTGRES_DIALECT, chooseQuery);
    } catch (MondrianException mExc) {
      fail(
          "Not expected any MondrianException but it occured: "
          + mExc.getLocalizedMessage());
    }
  }

  /**
   * ISSUE MONDRIAN-2335 If SqlQuery.CodeSet contains sql code
   * for both dialect="postgres" and dialect="generic",
   * the code for dialect="postgres"should be chosen. No error should be thrown
   *
   * @throws Exception
   */
  public void testSucces_CodeSetContainsCodeForBothPostgresAndGenericDialects()
    throws Exception
    {
    PostgreSqlDialect postgreSqlDialect = new PostgreSqlDialect(
        mockConnection(
            POSTGRESQL_PRODUCT_NAME,
            POSTGRESQL_PRODUCT_VERSION));
    codeSet = new SqlQuery.CodeSet();
    codeSet.put(POSTGRES_DIALECT, SQL_CODE_FOR_POSTGRES_DIALECT);
    codeSet.put(GENERIC_DIALECT, SQL_CODE_FOR_GENERIC_DIALECT);
    try {
      String chooseQuery = codeSet.chooseQuery(postgreSqlDialect);
      assertEquals(SQL_CODE_FOR_POSTGRES_DIALECT, chooseQuery);
    } catch (MondrianException mExc) {
      fail(
          "Not expected any MondrianException but it occured: "
          + mExc.getLocalizedMessage());
    }
  }

  /**
   * ISSUE MONDRIAN-2335 If SqlQuery.CodeSet contains sql code
   * for both dialect="postgres" and dialect="postgresql",
   * the code for dialect="postgres"should be chosen. No error should be thrown
   *
   * @throws Exception
   */
  public void
    testSucces_CodeSetContainsCodeForBothPostgresAndPostgresqlDialects()
      throws Exception
      {
    PostgreSqlDialect postgreSqlDialect = new PostgreSqlDialect(
        mockConnection(
            POSTGRESQL_PRODUCT_NAME,
            POSTGRESQL_PRODUCT_VERSION));
    codeSet = new SqlQuery.CodeSet();
    codeSet.put(POSTGRES_DIALECT, SQL_CODE_FOR_POSTGRES_DIALECT);
    codeSet.put(POSTGRESQL_DIALECT, SQL_CODE_FOR_POSTGRESQL_DIALECT);
    try {
      String chooseQuery = codeSet.chooseQuery(postgreSqlDialect);
      assertEquals(SQL_CODE_FOR_POSTGRES_DIALECT, chooseQuery);
    } catch (MondrianException mExc) {
      fail(
          "Not expected any MondrianException but it occured: "
          + mExc.getLocalizedMessage());
    }
  }

  /**
   * If SqlQuery.CodeSet contains sql code for dialect="generic" ,
   * the code for dialect="generic" should be chosen. No error should be thrown
   *
   * @throws Exception
   */
  public void testSucces_CodeSetContainsOnlyCodeForGenericlDialect()
    throws Exception
    {
    PostgreSqlDialect postgreSqlDialect = new PostgreSqlDialect(
        mockConnection(
            POSTGRESQL_PRODUCT_NAME,
            POSTGRESQL_PRODUCT_VERSION));
    codeSet = new SqlQuery.CodeSet();
    codeSet.put(GENERIC_DIALECT, SQL_CODE_FOR_GENERIC_DIALECT);
    try {
      String chooseQuery = codeSet.chooseQuery(postgreSqlDialect);
      assertEquals(SQL_CODE_FOR_GENERIC_DIALECT, chooseQuery);
    } catch (MondrianException mExc) {
      fail(
          "Not expected any MondrianException but it occured: "
          + mExc.getLocalizedMessage());
    }
  }

  /**
   * If SqlQuery.CodeSet contains no sql code with specified dialect at all
   * (even 'generic'), the MondrianException should be thrown.
   *
   * @throws Exception
   */
  public void testMondrianExceptionThrown_WhenCodeSetContainsNOCodeForDialect()
    throws Exception
    {
    PostgreSqlDialect postgreSqlDialect = new PostgreSqlDialect(
        mockConnection(
            POSTGRESQL_PRODUCT_NAME,
            POSTGRESQL_PRODUCT_VERSION));
    codeSet = new SqlQuery.CodeSet();
    try {
      String chooseQuery = codeSet.chooseQuery(postgreSqlDialect);
      fail(
          "Expected MondrianException but not occured");
      assertEquals(SQL_CODE_FOR_GENERIC_DIALECT, chooseQuery);
    } catch (MondrianException mExc) {
      assertEquals(
          MONDRIAN_ERROR_NO_GENERIC_VARIANT,
          mExc.getLocalizedMessage());
    }
  }

  private Connection mockConnection(
      String dbProductName, String dbProductVersion) throws Exception
      {
    DatabaseMetaData dbMetaDataMock = mock(DatabaseMetaData.class);
    when(dbMetaDataMock.getDatabaseProductName()).thenReturn(
        dbProductName != null ? dbProductName : EMPTY_NAME);
    when(dbMetaDataMock.getDatabaseProductVersion()).thenReturn(
        dbProductVersion != null ? dbProductVersion : EMPTY_NAME);
    Connection conectionMock = mock(Connection.class);
    when(conectionMock.getMetaData()).thenReturn(dbMetaDataMock);
    return conectionMock;
  }

}

// End CodeSetTest.java
