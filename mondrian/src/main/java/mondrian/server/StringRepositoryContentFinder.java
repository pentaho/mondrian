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


package mondrian.server;

/**
 * Implementation of {@link mondrian.server.RepositoryContentFinder} that always returns a constant string.
 *
 * @author Julian Hyde
 */
public class StringRepositoryContentFinder implements RepositoryContentFinder {
  private final String content;

  public StringRepositoryContentFinder( String content ) {
    this.content = content;
  }

  public String getContent() {
    return content;
  }


  public void shutdown() {
    // nothing to do
  }
}
