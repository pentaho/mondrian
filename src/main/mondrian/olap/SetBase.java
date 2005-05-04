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

import mondrian.olap.type.Type;
import mondrian.olap.type.SetType;

/**
 * Skeleton implementation of {@link Set} interface.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
class SetBase extends OlapElementBase implements Set {

    private static final Logger LOGGER = Logger.getLogger(SetBase.class);

    private String name;
    private final Exp exp;

    SetBase(String name, Exp exp) {
        this.name = name;
        this.exp = exp;
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    // from Element
    public Object getObject() {
        return null;
    }
    public String getUniqueName() {
        return "[" + name + "]";
    }
    public String getName() {
        return name;
    }
    public String getQualifiedName() {
        return null;
    }
    public String getDescription() {
        return null;
    }

    public int getCategory() {
        return Category.Set;
    }

    public Type getTypeX() {
        return new SetType(exp.getTypeX());
    }

    public Hierarchy getHierarchy() {
        return exp.getTypeX().getHierarchy();
    }

    public Dimension getDimension() {
        return getHierarchy().getDimension();
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
        return exp.dependsOn(dimension);
    }

    public Object evaluate(Evaluator evaluator) {
        return exp.evaluate(evaluator);
    }
}

// End SetBase.java
