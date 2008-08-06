/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.Collection;
import java.util.HashSet;

import mondrian.olap.FunTable;
import mondrian.olap.Syntax;
import mondrian.olap.Util;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;
import mondrian.spi.UserDefinedFunction;

import org.apache.log4j.Logger;

/**
 * Global function table contains builtin functions and global user-defined functions.
 *
 * @author Gang Chen
 * @version $Id$
 */
public class GlobalFunTable extends FunTableImpl {

    private static Logger logger = Logger.getLogger(GlobalFunTable.class);

    private static GlobalFunTable instance = new GlobalFunTable();

    public static GlobalFunTable instance() {
        return instance;
    }

    private GlobalFunTable() {
        super();
        init();
    }

    protected void defineFunctions() {
        final FunTable builtinFunTable = BuiltinFunTable.instance();
        final List<String> reservedWords = builtinFunTable.getReservedWords();
        for (String reservedWord : reservedWords) {
            defineReserved(reservedWord);
        }
        final List<Resolver> resolvers = builtinFunTable.getResolvers();
        for (Resolver resolver : resolvers) {
            define(resolver);
        }

        for (String className : lookupUdfImplClasses()) {
            defineUdf(className);
        }
    }


    private Collection<String> lookupUdfImplClasses() {
        ClassLoader cl = this.getClass().getClassLoader();
        List<URL> serviceUrls = new ArrayList<URL>();
        try {
            Enumeration<URL> serviceEnum = cl.getResources("META-INF/services/mondrian.spi.UserDefinedFunction");
            for (; serviceEnum.hasMoreElements();) {
                serviceUrls.add(serviceEnum.nextElement());
            }
        } catch (IOException e) {
            logger.warn("Error while finding service files for user-defined functions", e);
        }
        Set<String> classNames = new HashSet<String>();
        for (URL url : serviceUrls) {
            BufferedReader reader = null;
            try {
                reader =
                    new BufferedReader(new InputStreamReader(url.openStream(),
                        "UTF-8"));
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.length() > 0) {
                        if (line.charAt(0) == '#') {
                            continue;
                        }
                        int comment = line.indexOf('#');
                        if (comment != -1) {
                            line = line.substring(0, comment).trim();
                        }
                        classNames.add(line);
                    }
                }
            } catch (IOException e) {
                logger.warn("Error when loading service file '" + url + "'", e);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }
        return classNames;
    }

    /**
     * Defines a user-defined function in this table.
     *
     * <p>If the function is not valid, throws an error.
     *
     * @param className Name of the class which implements the function.
     *   The class must implement {@link mondrian.spi.UserDefinedFunction}
     *   (otherwise it is a user-error).
     */
    private void defineUdf(String className) {
        // Load class.
        final Class<?> udfClass;
        try {
            udfClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw MondrianResource.instance().UdfClassNotFound.ex("",className);
        }

        // Instantiate class with default constructor.
        final UserDefinedFunction udf = Util.createUdf(udfClass);

        // Validate function.
        validateFunction(udf);

        // Define function.
        define(new UdfResolver(udf));
    }

    /**
     * Throws an error if a user-defined function does not adhere to the
     * API.
     */
    private void validateFunction(final UserDefinedFunction udf) {
        // Check that the name is not null or empty.
        final String udfName = udf.getName();
        if (udfName == null || udfName.equals("")) {
            throw Util.newInternal("User-defined function defined by class '" +
                    udf.getClass() + "' has empty name");
        }
        // It's OK for the description to be null.
        //final String description = udf.getDescription();

        final Type[] parameterTypes = udf.getParameterTypes();
        for (int i = 0; i < parameterTypes.length; i++) {
            Type parameterType = parameterTypes[i];
            if (parameterType == null) {
                throw Util.newInternal("Invalid user-defined function '" +
                        udfName + "': parameter type #" + i +
                        " is null");
            }
        }

        // It's OK for the reserved words to be null or empty.
        //final String[] reservedWords = udf.getReservedWords();

        // Test that the function returns a sensible type when given the FORMAL
        // types. It may still fail when we give it the ACTUAL types, but it's
        // impossible to check that now.
        final Type returnType = udf.getReturnType(parameterTypes);
        if (returnType == null) {
            throw Util.newInternal("Invalid user-defined function '" +
                    udfName + "': return type is null");
        }
        final Syntax syntax = udf.getSyntax();
        if (syntax == null) {
            throw Util.newInternal("Invalid user-defined function '" +
                    udfName + "': syntax is null");
        }
    }


}

// End GlobalFunTable.java
