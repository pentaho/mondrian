/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.Member;

/**
 * This helper class is used by the Distinct(&lt;set&gt;) function to determine
 * whether or not elements of different types within a set are equal.
 */
public class MemberHelper {
    private Object mObject;
    public MemberHelper(Object entry) {
        if (entry == null || entry instanceof Member || entry instanceof Member[]) {
            mObject = entry;
        }
        else {
            throw new IllegalArgumentException("Expected Member or Member[]");
        }
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof MemberHelper)) {
            return false;
        }
        else if (this == obj) {
            return true;
        }

        MemberHelper mh = (MemberHelper)obj;

        if (this.mObject == null) {
            return mh.mObject == null;
        }
        else if (mh.mObject instanceof Member && mObject instanceof Member) {
            return ((Member)mObject).equals(mh.mObject);
        }
        else if (mh.mObject instanceof Member[] && mObject instanceof Member[]) {
            Member[] array1 = (Member[])mh.mObject;
            Member[] array2 = (Member[])this.mObject;

            if (array1.length != array2.length) {
                return false;
            }

            for (int idx = 0; idx < array1.length; idx++) {
                if (!array1[idx].equals(array2[idx])) {
                    return false;
                }
            }

            return true;
        }
        else {
            return false;
        }
    }

    public int hashCode() {
        if (mObject instanceof Member) {
            return mObject.hashCode();
        }
        else if (mObject instanceof Member[]) {
            int hash = 0;

            Member[] array = (Member[]) mObject;

            for (int idx = 0; idx < array.length; idx++) {
                hash ^= array[idx].hashCode();
            }

            return hash;
        }
        else {
            return mObject.hashCode();
        }
    }
}
