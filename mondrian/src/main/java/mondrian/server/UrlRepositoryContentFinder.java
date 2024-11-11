/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.server;

import mondrian.olap.Util;

import java.io.IOException;

/**
 * Implementation of {@link mondrian.server.RepositoryContentFinder} that reads from a URL.
 *
 * <p>The URL might be a string representation of a {@link java.net.URL}, such as 'file:/foo/bar/datasources.xml' or
 * 'http://server/datasources.xml', but it might also be the mondrian-specific URL format 'inline:...'. The content
 * of an inline URL is the rest of the string after the 'inline:' prefix.
 *
 * @author Julian Hyde
 */
public class UrlRepositoryContentFinder implements RepositoryContentFinder {
  protected final String url;

  /**
   * Creates a UrlRepositoryContentFinder.
   *
   * @param url URL of repository
   */
  public UrlRepositoryContentFinder( String url ) {
    if ( url == null ) {
      throw new AssertionError();
    }

    this.url = url;
  }

  public String getContent() {
    try {
      return Util.readURL( url, Util.toMap( System.getProperties() ) );
    } catch ( IOException e ) {
      throw new RuntimeException( e );
    }
  }

  public void shutdown() {
    // nothing to do
  }
}
