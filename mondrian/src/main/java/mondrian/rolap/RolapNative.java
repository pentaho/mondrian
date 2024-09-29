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


package mondrian.rolap;

import mondrian.olap.*;

import java.util.EventObject;

/**
 * A factory for {@link mondrian.olap.NativeEvaluator}.
 * If the instance returns null,
 * then the interpreter must compute the result itself.
 * If it returns a NativeEvaluator
 * the interpreter may choose to let the NativeEvaluator compute the result.
 *
 * <p>There exist multiple RolapNative implementations, e.g. for CrossJoin,
 * TopCount, Filter etc. If the arguments of these functions are simple enough
 * to be evaluated in SQL then a NativeEvaluator will be returned that performs
 * the computations in SQL. Otherwise null will be returned.
 */
public abstract class RolapNative {

    private boolean enabled;

    static class NativeEvent extends EventObject {
        private final NativeEvaluator neval;
        public NativeEvent(Object source, NativeEvaluator neval) {
            super(source);
            this.neval = neval;
        }
        NativeEvaluator getNativeEvaluator() {
            return neval;
        }
    }

    static class TupleEvent extends EventObject {
        private final TupleReader tupleReader;

        public TupleEvent(Object source, TupleReader tupleReader) {
            super(source);
            this.tupleReader = tupleReader;
        }

        TupleReader getTupleReader() {
            return tupleReader;
        }
    }

    interface Listener {
        void foundEvaluator(NativeEvent e);
        void foundInCache(TupleEvent e);
        void executingSql(TupleEvent e);
    }

    protected Listener listener;

    /**
     * If function can be implemented in SQL, returns a NativeEvaluator that
     * computes the result; otherwise returns null.
     */
    abstract NativeEvaluator createEvaluator(
        RolapEvaluator evaluator,
        FunDef fun,
        Exp[] args);

    /**
     * if enabled == false, then createEvaluator will always return null
     */
    boolean isEnabled() {
        return enabled;
    }

    void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    Listener getListener() {
        return listener;
    }

    void setListener(Listener listener) {
        this.listener = listener;
    }

    /**
     * Sets whether to use hard caching for testing.
     * When using soft references, we can not test caching
     * because things may be garbage collected during the tests.
     */
    abstract void useHardCache(boolean hard);
}

// End RolapNative.java
