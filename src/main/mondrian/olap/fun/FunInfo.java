/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.olap.fun;

import mondrian.olap.FunDef;
import mondrian.olap.Syntax;

/** 
 * Support class for the CmdRunner allowing one to view available functions and
 * their syntax.
 * 
 * @author Richard M. Emberson
 * @version 
 */
public class FunInfo implements Comparable {

    static FunInfo make(Resolver resolver) {
        FunInfo funInfo = null;
        if (resolver instanceof SimpleResolver) {
            FunDef funDef = ((SimpleResolver) resolver).getFunDef();
            funInfo = new FunInfo(funDef);

        } else if (resolver instanceof MultiResolver) {
            funInfo = new FunInfo((MultiResolver) resolver);

        } else {
            funInfo = new FunInfo(resolver);

        }
        return funInfo;
    }
    private final Syntax syntax;
    private final String name;
    private final String description;
    private final int[] returnTypes;
    private final int[][] parameterTypes;

    FunInfo(FunDef funDef) {
        this.syntax = funDef.getSyntax();
        this.name = funDef.getName();
        this.description = funDef.getDescription();
        this.returnTypes = new int[] { funDef.getReturnType() };
        this.parameterTypes = new int[][] { funDef.getParameterTypes() };
    }
    FunInfo(MultiResolver multiResolver) {
        this.syntax = multiResolver.getSyntax();
        this.name = multiResolver.getName();
        this.description = multiResolver.getDescription();

        String[] signatures = multiResolver.getSignatures();
        this.returnTypes = new int[signatures.length];
        this.parameterTypes = new int[signatures.length][];
        for (int i = 0; i < signatures.length; i++) {
            this.returnTypes[i] = FunUtil.decodeReturnType(signatures[i]);
            this.parameterTypes[i] = FunUtil.decodeParameterTypes(signatures[i]);
        }
    }
    FunInfo(Resolver resolver) {
        this.syntax = resolver.getSyntax();
        this.name = resolver.getName();
        this.description = resolver.getDescription();
        this.returnTypes = null;
        this.parameterTypes = null;
    }

    public String[] getSignatures() {
        if (this.parameterTypes == null) {
            return null;
        } 

        String[] sigs = new String[this.parameterTypes.length];
        for (int i = 0; i < sigs.length; i++) {
            sigs[i] = this.syntax.getSignature(this.name, this.returnTypes[i], 
                this.parameterTypes[i]);
        }
        return sigs;
    }
    /**
     * Returns the syntactic type of the function. */
    public Syntax getSyntax() {
        return this.syntax;
    }

    /**
     * Returns the name of this function.
     **/
    public String getName() {
        return this.name;
    }

    /**
     * Returns the description of this function.
     **/
    public String getDescription() {
        return this.description;
    }

    /**
     * Returns the type of value returned by this function. Values are the same
     * as those returned by {@link Exp#getType}.
     **/
    public int[] getReturnTypes() {
        return this.returnTypes;
    }

    /**
     * Returns the types of the arguments of this function. Values are the same
     * as those returned by {@link Exp#getType}. The 0<sup>th</sup>
     * argument of methods and properties are the object they are applied
     * to. Infix operators have two arguments, and prefix operators have one
     * argument.
     **/
    public int[][] getParameterTypes() {
        return this.parameterTypes;
    }


    public int compareTo(Object o) {
        FunInfo fi = (FunInfo) o;
        return this.name.compareTo(fi.name);
    }

}
