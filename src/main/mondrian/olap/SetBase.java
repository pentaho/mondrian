/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

import org.apache.log4j.Logger;

/**
 * Skeleton implementation of {@link Set} interface.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
class SetBase extends OlapElementBase implements Set {

    private static final Logger LOGGER = Logger.getLogger(SetBase.class);

    String name;
    Exp exp;

    SetBase(String name, Exp exp) {
        this.name = name;
        this.exp = exp;
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    // from Element
    public Object getObject() { return null; }
    public String getUniqueName() { return "[" + name + "]"; }
    public String getName() { return name; }
    public String getQualifiedName() { return null; }
    public String getDescription() { return null; }

    public int getType() {
        return Category.Set;
    }
    public boolean usesDimension(Dimension dimension) {
        return false;
    }
    public Hierarchy getHierarchy() {
        return exp.getHierarchy();
    }
    public OlapElement lookupChild(SchemaReader schemaReader, String s) {
        return null;
    }
    public void setName(String name) {
        this.name = name;
    }
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
    public void childrenAccept(Visitor visitor) {
    }
    public boolean dependsOn(Dimension dimension) {
        throw new UnsupportedOperationException();
    }
}

// End SetBase.java
