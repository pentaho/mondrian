/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

import mondrian.olap.type.Type;

import org.apache.log4j.Logger;

/**
 * Skeleton implementation of {@link NamedSet} interface.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 */
class SetBase extends OlapElementBase implements NamedSet {

    private static final Logger LOGGER = Logger.getLogger(SetBase.class);

    private String name;
    private final String uniqueName;
    private final Exp exp;

    SetBase(String name, Exp exp) {
        this.name = name;
        this.exp = exp;
        this.uniqueName = "[" + name + "]";        
    }

    public Object clone() {
        return new SetBase(name, (Exp) exp.clone());
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public String getUniqueName() {
        return uniqueName;
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

    public Hierarchy getHierarchy() {
        return exp.getType().getHierarchy();
    }

    public Dimension getDimension() {
        return getHierarchy().getDimension();
    }

    public OlapElement lookupChild(SchemaReader schemaReader, Id.Segment s) {
        return null;
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, Id.Segment s, MatchType matchType) {
        return null;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Exp getExp() {
        return exp;
    }

    public NamedSet validate(Validator validator) {
        Exp exp2 = validator.validate(exp, false);
        return new SetBase(name, exp2);
    }

    public Type getType() {
        return exp.getType();
    }
}

// End SetBase.java
