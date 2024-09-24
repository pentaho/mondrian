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

package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.spi.UserDefinedFunction;
import mondrian.util.ServiceDiscovery;

import java.util.List;

/**
 * Global function table contains builtin functions and global user-defined
 * functions.
 *
 * @author Gang Chen
 */
public class GlobalFunTable extends FunTableImpl {

    private static GlobalFunTable instance = new GlobalFunTable();
    static {
        instance.init();
    }

    public static GlobalFunTable instance() {
        return instance;
    }

    private GlobalFunTable() {
    }

    public void defineFunctions(Builder builder) {
        final FunTable builtinFunTable = BuiltinFunTable.instance();
        final List<String> reservedWords = builtinFunTable.getReservedWords();
        for (String reservedWord : reservedWords) {
            builder.defineReserved(reservedWord);
        }
        final List<Resolver> resolvers = builtinFunTable.getResolvers();
        for (Resolver resolver : resolvers) {
            builder.define(resolver);
        }

        for (Class<UserDefinedFunction> udfClass : lookupUdfImplClasses()) {
            defineUdf(
                builder,
                new UdfResolver.ClassUdfFactory(udfClass, null));
        }
    }

    private List<Class<UserDefinedFunction>> lookupUdfImplClasses() {
        final ServiceDiscovery<UserDefinedFunction> serviceDiscovery =
            ServiceDiscovery.forClass(UserDefinedFunction.class);
        return serviceDiscovery.getImplementor();
    }

    /**
     * Defines a user-defined function in this table.
     *
     * <p>If the function is not valid, throws an error.
     *
     * @param builder Builder
     * @param udfFactory Factory for UDF
     */
    private void defineUdf(
        Builder builder,
        UdfResolver.UdfFactory udfFactory)
    {
        // Instantiate class with default constructor.
        final UserDefinedFunction udf = udfFactory.create();

        // Validate function.
        validateFunction(udf);

        // Define function.
        builder.define(new UdfResolver(udfFactory));
    }

    /**
     * Throws an error if a user-defined function does not adhere to the
     * API.
     *
     * @param udf User defined function
     */
    private void validateFunction(final UserDefinedFunction udf) {
        // Check that the name is not null or empty.
        final String udfName = udf.getName();
        if (udfName == null || udfName.equals("")) {
            throw Util.newInternal(
                "User-defined function defined by class '"
                + udf.getClass() + "' has empty name");
        }
        // It's OK for the description to be null.
        //final String description = udf.getDescription();

        final Type[] parameterTypes = udf.getParameterTypes();
        for (int i = 0; i < parameterTypes.length; i++) {
            Type parameterType = parameterTypes[i];
            if (parameterType == null) {
                throw Util.newInternal(
                    "Invalid user-defined function '" + udfName
                    + "': parameter type #" + i + " is null");
            }
        }

        // It's OK for the reserved words to be null or empty.
        //final String[] reservedWords = udf.getReservedWords();

        // Test that the function returns a sensible type when given the FORMAL
        // types. It may still fail when we give it the ACTUAL types, but it's
        // impossible to check that now.
        final Type returnType = udf.getReturnType(parameterTypes);
        if (returnType == null) {
            throw Util.newInternal(
                "Invalid user-defined function '" + udfName
                + "': return type is null");
        }
        final Syntax syntax = udf.getSyntax();
        if (syntax == null) {
            throw Util.newInternal(
                "Invalid user-defined function '" + udfName
                + "': syntax is null");
        }
    }
}

// End GlobalFunTable.java
