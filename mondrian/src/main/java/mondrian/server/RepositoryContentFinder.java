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



package mondrian.server;

/**
 * Callback to get the content of the repository as an XML string.
 *
 * <p>Various implementations might use caching or storage in media other than a file system.
 *
 * @author Julian Hyde
 */
public interface RepositoryContentFinder {
  String getContent();


  void shutdown();
}
