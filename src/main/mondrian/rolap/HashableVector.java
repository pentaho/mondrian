/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/

package mondrian.rolap;
import java.util.*;

/**
 * A version of Vector which implements equals and hashCode, making it
 * usable as a compound hash key.
 **/
class HashableVector extends Vector
{
	// IMPLEMENT option to cache the hashCode when safe
	
	public boolean equals(Object other)
	{
		if (this == other) {
			return true;
		}
		if (!(other instanceof HashableVector)) {
			return false;
		}
		HashableVector otherVector = (HashableVector) other;
		if (otherVector.elementCount != elementCount) {
			return false;
		}
		for (int i = 0; i < elementCount; i++) {
			if (elementData[i] == null) {
				if (otherVector.elementData[i] == null) {
					continue;
				} else {
					return false;
				}
			}
			if (!elementData[i].equals(otherVector.elementData[i])) {
				return false;
			}
		}
		return true;
	}

	public int hashCode()
	{
		// IMPLEMENT something smarter...
		int hashCode = 0;
		for (int i = 0; i < elementCount; i++) {
			Object o = elementData[i];
			if (o == null) {
				continue;
			}
			hashCode += 13*o.hashCode();
		}
		return hashCode;
	}
}


// End HashableVector.java
