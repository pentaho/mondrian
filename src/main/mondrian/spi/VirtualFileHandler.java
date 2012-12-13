/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.spi;

import mondrian.olap.*;
import mondrian.util.ClassResolver;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;

/**
 * Reads virtual files specified by a URL.
 *
 * <p>The handler is instantiated based upon the value of the
 * "{@link MondrianProperties#VfsClass "mondrian.spi.virtualFileHandlerClass}"
 * property.</p>
 *
 * @author jhyde
 */
public interface VirtualFileHandler {
    Logger LOGGER = Logger.getLogger(VirtualFileHandler.class);

    String[] BUILTIN_IMPLEMENTATIONS = {
        "mondrian.spi.impl.ApacheVfsVirtualFileHandler",
        "mondrian.spi.impl.ApacheVfs2VirtualFileHandler",
        "mondrian.spi.impl.JavaNetVirtualFileHandler"
    };

    Util.Function0<VirtualFileHandler> FACTORY =
        new Util.Function0<VirtualFileHandler>() {
            public VirtualFileHandler apply() {
                String[] classNames = {
                    MondrianProperties.instance().VfsClass.get()
                };
                if (classNames[0] == null) {
                    // Fallback... try to instantiate standard
                    // implementations, in order. These will fail if
                    // required libraries are not on classpath.
                    classNames = BUILTIN_IMPLEMENTATIONS;
                    LOGGER.info(
                        "VirtualFileHandler: Property not set. Trying"
                        + "standard implementations");
                }
                for (String className : classNames) {
                    try {
                        VirtualFileHandler vfh =
                            ClassResolver.INSTANCE.instantiateSafe(className);
                        LOGGER.info(
                            "VirtualFileHandler: Using " + className);
                        return vfh;
                    } catch (Exception e) {
                        LOGGER.info(
                            "Failed to instantiate " + className,
                            e);
                    }
                }
                final RuntimeException e =
                    Util.newError(
                        "Could not instantiate VirtualFileHandler; tried "
                        + Arrays.toString(classNames));
                LOGGER.error("Giving up", e);
                throw e;
            }
        };

    /**
     * Gets the content of a file. File must exist and have content.
     *
     * @param url URL string
     * @return Contents of file as an input stream
     */
    InputStream readVirtualFile(String url) throws IOException;
}

// End VirtualFileHandler.java
