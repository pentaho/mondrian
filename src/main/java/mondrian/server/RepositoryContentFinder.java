/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.server;

/**
* Callback to get the content of the repository as an XML string.
 *
 * <p>Various implementations might use caching or storage in media other than
 * a file system.
 *
 * @author Julian Hyde
*/
public interface RepositoryContentFinder {
    String getContent();

    void shutdown();
}

// End RepositoryContentFinder.java
