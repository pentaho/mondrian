/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 January, 1999
*/

package mondrian.olap;
import mondrian.olap.type.Type;
import mondrian.mdx.MdxVisitor;

import java.io.PrintWriter;

/**
 * Multi-part identifier.
 */
public class Id
    extends ExpBase
    implements Cloneable {

    private final String[] names;
    private final boolean[] keys;

    /**
     * Creates an identifier containing a single part.
     *
     * @param name Name
     * @param key Whether the part represents a key value
     */
    public Id(String name, boolean key) {
        names = new String[] {name};
        keys = new boolean[] {key};
    }

    Id(String s) {
        this(s, false);
    }

    private Id(String[] names, boolean[] keys) {
        this.names = names;
        this.keys = keys;
    }

    public Object clone() {
        // This is immutable, so no need to clone.
        return this;
    }

    public int getCategory() {
        return Category.Unknown;
    }

    public Type getType() {
        // Can't give the type until we have resolved.
        throw new UnsupportedOperationException();
    }

    public String toString() {
        return Util.quoteMdxIdentifier(toStringArray());
    }

    public String[] toStringArray() {
        return (String[]) names.clone();
    }

    public String getElement(int i) {
        return names[i];
    }

    public Id append(String s, boolean key) {
        String[] newNames = new String[names.length + 1];
        boolean[] newKeys = new boolean[keys.length + 1];
        System.arraycopy(names, 0, newNames, 0, names.length);
        System.arraycopy(keys, 0, newKeys, 0, keys.length);
        newNames[newNames.length - 1] = s;
        newKeys[newKeys.length - 1] = key;
        return new Id(newNames, newKeys);
    }

    public void append(String s) {
        append(s, false);
    }

    public Exp accept(Validator validator) {
        if (names.length == 1) {
            final String s = names[0];
            if (validator.getFunTable().isReserved(s)) {
                return Literal.createSymbol(s.toUpperCase());
            }
        }
        final Exp element = Util.lookup(validator.getQuery(), names, true);
        if (element == null) {
            return null;
        }
        return element.accept(validator);
    }

    public Object accept(MdxVisitor visitor) {
        return visitor.visit(this);
    }

    public void unparse(PrintWriter pw) {
        for (int i = 0; i < names.length; i++) {
            String s = names[i];
            if (i > 0) {
                pw.print(".");
            }
            if (keys[i]) {
                pw.print("&[" + Util.mdxEncodeString(s) + "]");
            } else {
                pw.print("[" + Util.mdxEncodeString(s) + "]");
            }
        }
    }

}

// End Id.java
