/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2021 Sergei Semenkov
 * Copyright (C) 2024 Hitachi Vantara. All Rights Reserved.
 */

package mondrian.olap;

import java.util.List;

@SuppressWarnings( { "java:S1068" } )
public class DmvQuery extends QueryPart {
  private final String tableName;
  private final List<String> columns;
  private final Exp whereExpression;

  public DmvQuery( String tableName, List<String> columns, Exp whereExpression ) {
    this.tableName = tableName;
    this.columns = columns;
    this.whereExpression = whereExpression;
  }

  public String getTableName() {
    return this.tableName;
  }

  public Exp getWhereExpression() {
    return this.whereExpression;
  }
}
