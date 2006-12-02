/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.ArrayList;
import java.util.List;

import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.NativeEvaluator;

/**
 * Composite of {@link RolapNative}s. Uses chain of responsibility
 * to select the appropriate {@link RolapNative} evaluator.
 */
public class RolapNativeRegistry extends RolapNative {

    private List<RolapNative> natives = new ArrayList<RolapNative>();

    public RolapNativeRegistry() {
        super.setEnabled(true);
        register(new RolapNativeCrossJoin());
        register(new RolapNativeTopCount());
        register(new RolapNativeFilter());
    }

    /**
     * Returns the matching NativeEvaluator or null if <code>fun</code> can not
     * be executed in SQL for the given context and arguments.
     */
    public NativeEvaluator createEvaluator(
        RolapEvaluator evaluator, FunDef fun, Exp[] args)
    {
        if (!isEnabled()) {
            return null;
        }
        for (RolapNative rn : natives) {
            NativeEvaluator ne = rn.createEvaluator(evaluator, fun, args);
            if (ne != null) {
                if (listener != null) {
                    NativeEvent e = new NativeEvent(this, ne);
                    listener.foundEvaluator(e);
                }
                return ne;
            }
        }
        return null;
    }

    public void register(RolapNative rn) {
        natives.add(rn);
    }

    /** for testing */
    void setListener(Listener listener) {
        super.setListener(listener);
        for (RolapNative rn : natives) {
            rn.setListener(listener);
        }
    }
    
    /** for testing */
    void useHardCache(boolean hard) {
        for (RolapNative rn : natives) {
            rn.useHardCache(hard);
        }
    }
    
}

// End RolapNativeRegistry.java
