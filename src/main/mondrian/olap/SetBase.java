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
 * Skeleton implementation of {@link NamedSet} interface.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
class SetBase extends OlapElementBase implements NamedSet {

    private static final Logger LOGGER = Logger.getLogger(SetBase.class);

    private String name;
    private final Exp exp;

    SetBase(String name, Exp exp) {
        this.name = name;
        this.exp = exp;
    }

    public Object clone() {
        return new SetBase(name, (Exp) exp.clone());
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

    public Exp accept(Validator validator) {
        // A set is sometimes used in more than one cube. So, clone the
        // expression and re-validate every time it is used.
        //
        // But keep the expression wrapped in a NamedSet, so that the
        // expression is evaluated once per query. (We don't want the
        // expression to be evaluated context-sensitive.)
        final Exp clonedExp = (Exp) exp.clone();
        final Exp exp3 = clonedExp.accept(validator);
        return new SetBase(name, exp3);
    }

    public void childrenAccept(Visitor visitor) {
    }

    public boolean dependsOn(Dimension dimension) {
        return exp.dependsOn(dimension);
    }

    public Object evaluate(Evaluator evaluator) {
        return evaluator.evaluateNamedSet(name, exp);
    }
}

// End SetBase.java
