// $Id$
package mondrian.olap.fun;

import java.util.*;

import mondrian.olap.*;

/**
 * Interface to build a customized function table, selecting functions from the set of
 * supported functions in BuiltInFunTable instance.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class CustomizedFunctionTable extends FunTableImpl {

    Set<String> supportedBuiltInFunctions;
    Set<FunDef> specialFunctions;
    
    public CustomizedFunctionTable(Set<String> buildInFunctions) {
        supportedBuiltInFunctions = buildInFunctions;
        this.specialFunctions = new HashSet<FunDef>();
    }

    public CustomizedFunctionTable(Set<String> buildInFunctions, Set<FunDef> specialFunctions) {
        supportedBuiltInFunctions = buildInFunctions;
        this.specialFunctions = specialFunctions;
    }

    protected void defineFunctions() {
        final FunTable builtinFunTable = BuiltinFunTable.instance();
        
        // Includes all the keywords form builtin function table
        for (String reservedWord : builtinFunTable.getReservedWords()) {
            defineReserved(reservedWord);
        }
        
        // Add supported builtin functions
        for (Resolver resolver : builtinFunTable.getResolvers()) {
            if (supportedBuiltInFunctions.contains(resolver.getName())) {
                define(resolver);
            }
        }
        
        // Add special function definitions
        for (FunDef funDef : specialFunctions) {
            define(funDef);
        }
    }
}

// End CustomizedFunctionTable.java
