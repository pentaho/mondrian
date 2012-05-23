/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import mondrian.olap.Util;
import mondrian.spi.VirtualFileHandler;

import org.apache.commons.vfs2.*;

import java.io.*;

/**
 * Implementation of {@link VirtualFileHandler} that uses
 * <a href="http://commons.apache.org/vfs/">Apache VFS version 2</a>.
 *
 * @see ApacheVfsVirtualFileHandler
 *
 * @author jhyde
 */
public class ApacheVfs2VirtualFileHandler implements VirtualFileHandler {
    public InputStream readVirtualFile(String url)
        throws FileSystemException
    {
        // Treat catalogUrl as an Apache VFS (Virtual File System) URL.
        // VFS handles all of the usual protocols (http:, file:)
        // and then some.
        FileSystemManager fsManager = VFS.getManager();
        if (fsManager == null) {
            throw Util.newError("Cannot get virtual file system manager");
        }

        File userDir = new File("").getAbsoluteFile();
        FileObject file = fsManager.resolveFile(userDir, url);
        FileContent fileContent = null;
        try {
            if (!file.isReadable()) {
                throw Util.newError("Virtual file is not readable: " + url);
            }

            fileContent = file.getContent();
        } finally {
            file.close();
        }

        if (fileContent == null) {
            throw Util.newError("Cannot get virtual file content: " + url);
        }

        return fileContent.getInputStream();
    }
}

// End ApacheVfs2VirtualFileHandler.java
