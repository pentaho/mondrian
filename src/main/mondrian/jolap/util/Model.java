/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 25, 2002
*/
package mondrian.jolap.util;

import mondrian.olap.Util;

import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

import org.apache.log4j.Logger;

/**
 * <code>Model</code> attempts to deduce an object model consisting of
 * entities, attributes and relationships using Java reflection.
 *
 * @author jhyde
 * @since Dec 25, 2002
 * @version $Id$
 **/
public class Model {
    private static final Logger LOGGER = Logger.getLogger(Model.class);

    private static Model instance;
    private HashMap mapClassToEntity = new HashMap();
    private static final Entity noEntity = new Entity(null, Object.class);
    private static final Relationship noRelationship = new Relationship(noEntity, null, null);
    private static final Attribute[] emptyAttributeArray = new Attribute[0];
    private static final Relationship[] emptyRelationshipArray = new Relationship[0];
    private static final Class[] emptyClassArray = new Class[0];
    static int nextOrdinal;

    /** Returns the singleton instance. **/
    public static synchronized Model instance() {
        if (instance == null) {
            instance = new Model();
        }
        return instance;
    }

    public Entity getEntity(Object o) {
        return getEntityForClass(o.getClass());
    }

    private Entity getEntityForClass(Class clazz) {
        Entity entity = (Entity) mapClassToEntity.get(clazz);
        if (entity == null) {
            entity = deduceEntity(clazz);
            if (entity == null) {
                entity = noEntity;
            }
            mapClassToEntity.put(clazz, entity);
        }
        if (entity == noEntity) {
            return null;
        }
        return entity;
    }

    private Entity deduceEntity(Class clazz) {
        return new Entity(this, clazz);
    }

    private static String getShortName(Class clazz) {
        final String clazzName = clazz.toString();
        final int i = clazzName.lastIndexOf(".");
        if (i < 0) {
            return clazzName;
        } else {
            return clazzName.substring(i + 1);
        }
    }

    static class Entity {
        Model model;
        Class clazz;
        int ordinal;

        Entity(Model model, Class clazz) {
            this.model = model;
            this.clazz = clazz;
            this.ordinal = nextOrdinal++;
        }

        public String toString() {
            return getShortName(clazz);
        }

        public String getDescription() {
            return toString() + " (class " + clazz.getName() + ")";
        }

        Model getModel() {
            return model;
        }

        Attribute[] getDeclaredAttributes() {
            ArrayList attributeList = new ArrayList();
            ArrayList relationshipList = new ArrayList();
            computeMembers(attributeList, relationshipList);
            return (Attribute[]) attributeList.toArray(emptyAttributeArray);
        }

        Attribute[] getAttributes() {
            final Class[] ancestors = getAncestors(clazz);
            ArrayList attributeList = new ArrayList();
            ArrayList relationshipList = new ArrayList();
            for (int i = 0; i < ancestors.length; i++) {
                Class ancestor = ancestors[i];
                getModel().getEntityForClass(ancestor).computeMembers(attributeList, relationshipList);
            }
            return (Attribute[]) attributeList.toArray(emptyAttributeArray);
        }

        Relationship[] getRelationships() {
            final Class[] ancestors = getAncestors(clazz);
            ArrayList attributeList = new ArrayList();
            ArrayList relationshipList = new ArrayList();
            for (int i = 0; i < ancestors.length; i++) {
                Class ancestor = ancestors[i];
                getModel().getEntity(ancestor).computeMembers(attributeList, relationshipList);
            }
            return (Relationship[]) relationshipList.toArray(emptyRelationshipArray);
        }

        private Class[] getAncestors(Class clazz) {
            HashSet ancestors = new HashSet();
            ancestors.add(clazz);
            ArrayList extra = new ArrayList();
            while (true) {
                for (Iterator iterator = ancestors.iterator(); iterator.hasNext();) {
                    Class ancestor = (Class) iterator.next();
                    final Class superclass = ancestor.getSuperclass();
                    if (superclass != null &&
                            !ancestors.contains(superclass)) {
                        extra.add(superclass);
                    }
                    final Class[] interfaces = ancestor.getInterfaces();
                    for (int i = 0; i < interfaces.length; i++) {
                        Class anInterface = interfaces[i];
                        if (!ancestors.contains(anInterface)) {
                            extra.add(anInterface);
                        }
                    }
                }
                if (extra.isEmpty()) {
                    return (Class[]) ancestors.toArray(emptyClassArray);
                }
                ancestors.addAll(extra);
                extra.clear();
            }
        }

        Relationship[] getDeclaredRelationships() {
            ArrayList attributeList = new ArrayList();
            ArrayList relationshipList = new ArrayList();
            computeMembers(attributeList, relationshipList);
            return (Relationship[]) relationshipList.toArray(emptyRelationshipArray);
        }

        private void computeMembers(ArrayList attributeList, ArrayList relationshipList) {
            final int clazzModifiers = clazz.getModifiers();
            if (!Modifier.isPublic(clazzModifiers)) {
                // Somewhat controversial. But public members of a non-public
                // class are not callable. If they happen to implement an
                // interface, we will see them again.
                return;
            }
            // Each public, non-static field "x" is an attribute.
            Field[] fields = clazz.getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                final int modifiers = field.getModifiers();
                if (Modifier.isPublic(modifiers) &&
                        !Modifier.isStatic(modifiers)) {
                    attributeList.add(new FieldAttribute(this, field, field.getName()));
                }
            }
            // Each public, non-static method "getFoo()" is an attribute "foo",
            // unless there is also a method "addFoo()".
            final Method[] declaredMethods = clazz.getDeclaredMethods();
            HashSet methodNames = new HashSet();
            for (int i = 0; i < declaredMethods.length; i++) {
                Method declaredMethod = declaredMethods[i];
                methodNames.add(declaredMethod.getName());
            }
            for (int i = 0; i < declaredMethods.length; i++) {
                Method method = declaredMethods[i];
                final int modifiers = method.getModifiers();
                if (!Modifier.isPublic(modifiers) ||
                        Modifier.isStatic(modifiers)) {
                    continue;
                }
                final String name = method.getName();
                if (name.startsWith("get")) {
                    if (method.getParameterTypes().length > 0) {
                        continue;
                    }
                    if (method.getReturnType() == void.class) {
                        continue;
                    }
                    String suffix = name.substring("add".length());
                    if (methodNames.contains("remove" + suffix)) {
                        // There is a "remove" method, so this is probably a
                        // relationship accessor, not an attribute.
                        continue;
                    }
                    String lowerSuffix = suffix.substring(0,1).toLowerCase() +
                            suffix.substring(1);
                    attributeList.add(new MethodAttribute(this, method, lowerSuffix));
                } else if (name.startsWith("remove")) {
                    if (method.getParameterTypes().length != 1) {
                        continue;
                    }
                    if (method.getReturnType() != void.class) {
                        continue;
                    }
                    String suffix = name.substring("remove".length());
                    String lowerSuffix = suffix.substring(0,1).toLowerCase() +
                            suffix.substring(1);
                    relationshipList.add(new Relationship(this, method, lowerSuffix));
                }
            }
        }
    }

    static class Member {
        Entity entity;
        java.lang.reflect.Member member;
        String name;

        Member(Entity entity, java.lang.reflect.Member member, String name) {
            this.entity = entity;
            this.member = member;
            this.name = name;
        }

        protected Model getModel() {
            return entity.model;
        }

        public String toString() {
            return entity + "." + name;
        }
    }

    static abstract class Attribute extends Member {
        Attribute(Entity entity, java.lang.reflect.Member member, String name) {
            super(entity, member, name);
        }

        public String getDescription() {
            return toString() + ": " + getShortName(getType());
        }

        abstract Class getType();

        public abstract Object getValue(Object o);
    }

    static class FieldAttribute extends Attribute {
        FieldAttribute(Entity entity, Field field, String name) {
            super(entity, field, name);
        }

        Class getType() {
            return getField().getType();
        }

        private Field getField() {
            return (Field) member;
        }

        public Object getValue(Object o) {
            try {
                return getField().get(o);
            } catch (IllegalArgumentException e) {
                throw Util.newInternal(e, "Error while printing attribute");
            } catch (IllegalAccessException e) {
                throw Util.newInternal(e, "Error while printing attribute");
            }
        }
    }

    static class MethodAttribute extends Attribute {
        MethodAttribute(Entity entity, Method method, String name) {
            super(entity, method, name);
        }

        Class getType() {
            return getMethod().getReturnType();
        }

        private Method getMethod() {
            return (Method) member;
        }

        public Object getValue(Object o) {
            try {
                return getMethod().invoke(o, null);
            } catch (IllegalAccessException e) {
                throw Util.newInternal(e, "Error while getting attribute value");
            } catch (IllegalArgumentException e) {
                throw Util.newInternal(e, "Error while getting attribute value");
            } catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof UnsupportedOperationException) {
                    return null;
                }
                throw Util.newInternal(e, "Error while getting attribute value");
            }
        }
    }

    static class Relationship extends Member {
        private Relationship inverse;
        private Method getMethod;

        Relationship(Entity entity, java.lang.reflect.Member member, String name) {
            super(entity, member, name);
            if (member != null) {
                final String getMethodName = "get" + member.getName().substring("remove".length());
                try {
                    this.getMethod = entity.clazz.getMethod(getMethodName, emptyClassArray);
                } catch (NoSuchMethodException e) {
                    throw Util.newInternal(e, "Error while traversing relationship");
                } catch (SecurityException e) {
                    throw Util.newInternal(e, "Error while traversing relationship");
                }
            }
        }

        public String getDescription() {
            return toString() + ": " + getShortName(getTargetType()) +
                    (inverse == null ? "" : " (inverse " + inverse.name + ")");
        }

        Class getTargetType() {
            if (member instanceof Method) {
                final Method method = (Method) member;
                Util.assertTrue(method.getName().startsWith("remove") &&
                        method.getParameterTypes().length == 1);
                return method.getParameterTypes()[0];
            } else {
                throw Util.newInternal(member + " should be a Method");
            }
        }

        Relationship getInverse() {
            if (inverse == null) {
                inverse = computeInverse();
            }
            if (inverse == noRelationship) {
                return null;
            }
            return inverse;
        }

        private Relationship computeInverse() {
            Class targetClass = getTargetType();
            Entity targetEntity = getModel().getEntityForClass(targetClass);
            Relationship[] relationships = targetEntity.getDeclaredRelationships();
            Member candidate = null;
            int candidateCount = 0;
            for (int i = 0; i < relationships.length; i++) {
                Relationship relationship = relationships[i];
                if (relationship.getTargetType() == entity.clazz) {
                    candidate = relationship;
                    candidateCount++;
                }
            }
            Attribute[] attributes = targetEntity.getDeclaredAttributes();
            for (int i = 0; i < attributes.length; i++) {
                Attribute attribute = attributes[i];
                if (attribute.getType() == entity.clazz) {
                    candidate = attribute;
                    candidateCount++;
                }
            }
            if (candidateCount == 1) {
                return (Relationship) candidate;
            } else {
                LOGGER.debug("Found " + candidateCount + " candidates for inverse of " + this);
                return null;
            }
        }

        boolean isContains() {
            return isMany() && entity.ordinal < getModel().getEntity(getTargetType()).ordinal;
        }

        private boolean isMany() {
            return member.getName().startsWith("remove");
        }

        public Collection getTargets(Object o) {
            try {
                Object result = getMethod.invoke(o, null);
                if (result instanceof Collection) {
                    return (Collection) result;
                } else {
                    throw Util.newInternal("Cannot convert " +
                            result.getClass() + " in to a collection");
                }
            } catch (IllegalAccessException e) {
                throw Util.newInternal(e, "Error while traversing relationship");
            } catch (IllegalArgumentException e) {
                throw Util.newInternal(e, "Error while traversing relationship");
            } catch (InvocationTargetException e) {
                throw Util.newInternal(e, "Error while traversing relationship");
            }
        }
    }

    public static void main(String[] args) {
        foo(Class.class);
    }

    public static void foo(Object o) {
        Entity entity = Model.instance().getEntity(o);
        final Attribute[] attributes = entity.getAttributes();
        for (int i = 0; i < attributes.length; i++) {
            Attribute attribute = attributes[i];
            LOGGER.debug("Attribute: " + attribute.getDescription());
                    }
        final Relationship[] relationships = entity.getRelationships();
        for (int i = 0; i < relationships.length; i++) {
            Relationship relationship = relationships[i];
            LOGGER.debug("Relationship: " + relationship.getDescription());
        }
    }

    /**
     * Serializes an object, its attributes, and contained objects as XML.
     */
    public void toXML(Object o, PrintWriter pw) {
        toXML(o, pw, new HashSet(), 0);
    }

    private void toXML(Object o, PrintWriter pw, HashSet active, int indent) {
        Entity entity = Model.instance().getEntity(o);
        for (int i = 0; i < indent; i++) {
            pw.print("\t");
        }
        pw.print("<" + entity);
        int relationshipCount = 0;
        final Attribute[] attributes = entity.getAttributes();
        for (int i = 0; i < attributes.length; i++) {
            Attribute attribute = attributes[i];
            final Object value = attribute.getValue(o);
            if (value != null) {
                pw.print(" " + attribute.name + "=\"" + value + "\"");
            }
        }
        final Relationship[] relationships = entity.getRelationships();
        for (int i = 0; i < relationships.length; i++) {
            if (relationshipCount++ == 0) {
                pw.println(">");
            }
            Relationship relationship = relationships[i];
            Collection targets = relationship.getTargets(o);
            for (Iterator iterator = targets.iterator(); iterator.hasNext();) {
                Object target = iterator.next();
                if (active.add(target)) {
                    toXML(target, pw, active, indent + 1);
                    active.remove(target);
                }
            }
        }
        if (relationshipCount == 0) {
            pw.println("/>");
        } else {
            for (int i = 0; i < indent; i++) {
                pw.print("\t");
            }
            pw.println("</" + entity + ">");
        }
    }
}

// End Model.java
