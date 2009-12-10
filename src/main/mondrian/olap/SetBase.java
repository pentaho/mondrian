/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

import mondrian.olap.type.*;

import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Skeleton implementation of {@link NamedSet} interface.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 */
public class SetBase extends OlapElementBase implements NamedSet {

    private static final Logger LOGGER = Logger.getLogger(SetBase.class);

    private String name;
    private Map<String, Annotation> annotationMap;
    private String description;
    private final String uniqueName;
    private Exp exp;
    private boolean validated;

    /**
     * Creates a SetBase.
     *
     * @param name Name
     * @param caption Caption
     * @param description Description
     * @param exp Expression
     * @param validated Whether has been validated
     * @param annotationMap Annotations
     */
    SetBase(
        String name,
        String caption,
        String description,
        Exp exp,
        boolean validated,
        Map<String, Annotation> annotationMap)
    {
        this.name = name;
        this.annotationMap = annotationMap;
        this.caption = caption;
        this.description = description;
        this.exp = exp;
        this.validated = validated;
        this.uniqueName = "[" + name + "]";
    }

    public Map<String, Annotation> getAnnotationMap() {
        return annotationMap;
    }

    public String getNameUniqueWithinQuery() {
        return System.identityHashCode(this) + "";
    }

    public boolean isDynamic() {
        return false;
    }

    public Object clone() {
        return new SetBase(
            name, caption, description, exp.clone(), validated, annotationMap);
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
        return description;
    }

    public Hierarchy getHierarchy() {
        return exp.getType().getHierarchy();
    }

    public Dimension getDimension() {
        return getHierarchy().getDimension();
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, Id.Segment s, MatchType matchType)
    {
        return null;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setAnnotationMap(Map<String, Annotation> annotationMap) {
        this.annotationMap = annotationMap;
    }

    public Exp getExp() {
        return exp;
    }

    public NamedSet validate(Validator validator) {
        if (!validated) {
            exp = validator.validate(exp, false);
            validated = true;
        }
        return this;
    }

    public Type getType() {
        Type type = exp.getType();
        if (type instanceof MemberType
            || type instanceof TupleType)
        {
            // You can use a member or tuple as the expression for a set. It is
            // implicitly converted to a set. The expression may not have been
            // converted yet, so we wrap the type here.
            type = new SetType(type);
        }
        return type;
    }
}

// End SetBase.java
