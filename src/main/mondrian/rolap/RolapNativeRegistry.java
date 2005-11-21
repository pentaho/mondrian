/*
 //This software is subject to the terms of the Common Public License
 //Agreement, available at the following URL:
 //http://www.opensource.org/licenses/cpl.html.
 //Copyright (C) 2004-2005 TONBELLER AG
 //All Rights Reserved.
 //You must accept the terms of that agreement to use this software.
 */
package mondrian.rolap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.NativeEvaluator;

/**
 * registry fro RoalpNativeEvaluator's. Uses chain of responsibility
 * to select the appropriate evaluator.
 */
public class RolapNativeRegistry {

    private List evaluators = new ArrayList();
    private boolean enabled = true;


    public RolapNativeRegistry() {
        register(new RolapNativeCrossJoin());
        //register(new RolapNativeTopCount());
    }

    public NativeEvaluator findEvaluator(FunDef fun, Evaluator evaluator, Exp[] args) {
        if (!enabled)
            return null;
        RolapEvaluator revaluator = (RolapEvaluator) evaluator;
        for (Iterator it = evaluators.iterator(); it.hasNext();) {
            RolapNative rn = (RolapNative) it.next();
            NativeEvaluator ne = rn.createEvaluator(revaluator, fun, args);
            if (ne != null)
                return ne;
        }
        return null;
    }

    public void register(RolapNative rn) {
        evaluators.add(rn);
    }

    /** allows to disable native crossjoin evaluation for testing purposes */
    boolean isEnabled() {
        return enabled;
    }

    void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
