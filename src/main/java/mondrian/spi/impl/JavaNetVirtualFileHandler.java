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

import mondrian.spi.VirtualFileHandler;

import java.io.*;
import java.net.URL;

/**
 * Implementation of {@link mondrian.spi.VirtualFileHandler} that uses
 * {@link java.net.URL#openStream()}.
 *
 * @author jhyde
 */
public class JavaNetVirtualFileHandler implements VirtualFileHandler {
    public InputStream readVirtualFile(String url) throws IOException {
        return new URL(url).openStream();
    }
}

// End JavaNetVirtualFileHandler.java
