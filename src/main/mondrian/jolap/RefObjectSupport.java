/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 23, 2002
*/
package mondrian.jolap;

import javax.jmi.reflect.*;
import java.util.Collection;
import java.util.List;

/**
 * Abstract implementation of {@link RefObject} and several other JMI
 * reflection interfaces.
 *
 * @author jhyde
 * @since Dec 23, 2002
 * @version $Id$
 **/
abstract class RefObjectSupport implements RefObject {
    public boolean refIsInstanceOf(RefObject refObject, boolean b) {
        throw new UnsupportedOperationException();
    }

    public RefClass refClass() {
        throw new UnsupportedOperationException();
    }

    public RefFeatured refImmediateComposite() {
        throw new UnsupportedOperationException();
    }

    public RefFeatured refOutermostComposite() {
        throw new UnsupportedOperationException();
    }

    public void refDelete() {
        throw new UnsupportedOperationException();
    }

    public void refSetValue(RefObject refObject, Object o) {
        throw new UnsupportedOperationException();
    }

    public void refSetValue(String s, Object o) {
        throw new UnsupportedOperationException();
    }

    public Object refGetValue(RefObject refObject) {
        throw new UnsupportedOperationException();
    }

    public Object refGetValue(String s) {
        throw new UnsupportedOperationException();
    }

    public Object refInvokeOperation(RefObject refObject, List list) throws RefException {
        throw new UnsupportedOperationException();
    }

    public Object refInvokeOperation(String s, List list) throws RefException {
        throw new UnsupportedOperationException();
    }

    public RefObject refMetaObject() {
        throw new UnsupportedOperationException();
    }

    public RefPackage refImmediatePackage() {
        throw new UnsupportedOperationException();
    }

    public RefPackage refOutermostPackage() {
        throw new UnsupportedOperationException();
    }

    public String refMofId() {
        throw new UnsupportedOperationException();
    }

    public Collection refVerifyConstraints(boolean b) {
        throw new UnsupportedOperationException();
    }
}

// End RefObjectSupport.java
