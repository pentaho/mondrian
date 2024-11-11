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


package mondrian.olap.fun;

import mondrian.olap.FunDef;
import mondrian.olap.FunTable;

import java.util.HashSet;
import java.util.Set;

/**
 * Interface to build a customized function table, selecting functions from the
 * set of supported functions in an instance of {@link BuiltinFunTable}.
 *
 * @author Rushan Chen
 */
public class CustomizedFunctionTable extends FunTableImpl {

    Set<String> supportedBuiltinFunctions;
    Set<FunDef> specialFunctions;

    public CustomizedFunctionTable(Set<String> builtinFunctions) {
        supportedBuiltinFunctions = builtinFunctions;
        this.specialFunctions = new HashSet<FunDef>();
    }

    public CustomizedFunctionTable(
        Set<String> builtinFunctions,
        Set<FunDef> specialFunctions)
    {
        this.supportedBuiltinFunctions = builtinFunctions;
        this.specialFunctions = specialFunctions;
    }

    public void defineFunctions(Builder builder) {
        final FunTable builtinFunTable = BuiltinFunTable.instance();

        // Includes all the keywords form builtin function table
        for (String reservedWord : builtinFunTable.getReservedWords()) {
            builder.defineReserved(reservedWord);
        }

        // Add supported builtin functions
        for (Resolver resolver : builtinFunTable.getResolvers()) {
            if (supportedBuiltinFunctions.contains(resolver.getName())) {
                builder.define(resolver);
            }
        }

        // Add special function definitions
        for (FunDef funDef : specialFunctions) {
            builder.define(funDef);
        }
    }
}

// End CustomizedFunctionTable.java
