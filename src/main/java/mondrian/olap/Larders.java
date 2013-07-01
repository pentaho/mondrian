/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.olap;

import mondrian.util.Pair;

import org.olap4j.impl.*;
import org.olap4j.metadata.NamedList;

import java.util.*;

/**
 * Implementations and helpers for {@link Larder}.
 */
public class Larders {
    public static final Locale DEFAULT_LOCALE = Locale.getDefault();
    public static final Larder EMPTY = EmptyLarder.INSTANCE;

    private static final LocaleProp DEFAULT_LOCALE_CAPTION =
        LocaleProp.lookup(
            DEFAULT_LOCALE.toString(), LocalizedProperty.CAPTION);
    private static final LocaleProp DEFAULT_LOCALE_DESCRIPTION =
        LocaleProp.lookup(
            DEFAULT_LOCALE.toString(), LocalizedProperty.DESCRIPTION);

    private Larders() {
        throw new AssertionError("do not instantiate!");
    }

    public static Larder create(
        String name,
        String caption,
        String description)
    {
        return create(
            Olap4jUtil.<MondrianDef.Annotation>emptyNamedList(),
            name,
            caption,
            description,
            Collections.<Resource>emptyList());
    }

    public static Larder create(
        NamedList<MondrianDef.Annotation> annotations,
        String name,
        String caption,
        String description)
    {
        return create(
            annotations, name, caption, description,
            Collections.<Resource>emptyList());
    }

    public static Larder create(
        NamedList<MondrianDef.Annotation> annotations,
        String name,
        String caption,
        String description,
        List<Resource> resources)
    {
        if (annotations == null) {
            annotations = Olap4jUtil.emptyNamedList();
        }
        if (resources == null) {
            resources = Collections.emptyList();
        }
        if (caption != null && caption.equals(name)) {
            caption = null;
        }
        int n = annotations.size()
            + (caption != null ? 1 : 0)
            + (description != null ? 1 : 0)
            + resources.size();
        switch (n) {
        case 0:
            return EmptyLarder.INSTANCE;
        case 1:
            if (caption != null) {
                return ofCaption(caption);
            }
            // fall through
        default:
            final LarderBuilder builder = new LarderBuilder();
            builder.addAll(annotations);
            builder.caption(caption);
            builder.description(description);
            builder.addAll(resources);
            return builder.build();
        }
    }

    public static String get(
        Named named,
        Larder larder,
        LocalizedProperty prop,
        Locale locale)
    {
        final String value = larder.get(prop, locale);
        if (value == null
            && named != null
            && prop == LocalizedProperty.CAPTION)
        {
            return named.getName();
        }
        return value;
    }

    /** Returns a Larder that contains a caption and nothing else, or
     * the empty Larder if caption is null or is the same as name. */
    public static Larder ofCaption(String name, String caption) {
        if (caption != null && caption.equals(name)) {
            caption = null;
        }
        return ofCaption(caption);
    }

    /** Returns a Larder that contains a caption and nothing else. */
    public static Larder ofCaption(String caption) {
        if (caption == null) {
            return EmptyLarder.INSTANCE;
        } else {
            return new CaptionLarder(caption);
        }
    }

    /** Creates a Larder that applies a prefix to descriptions and captions. */
    public static Larder prefix(
        Larder larder,
        final String source,
        final String name)
    {
        if (source == null || name == null || source.equals(name)) {
            return larder;
        }
        return new DelegatingLarder((LarderImpl) larder) {
            public String get(LocalizedProperty prop, Locale locale) {
                final String value = super.get(prop, locale);
                if (value != null) {
                    return name + "." + value;
                }
                return value;
            }

            @Override
            public Map<Pair<Locale, LocalizedProperty>, String> translations() {
                //noinspection unchecked
                final Map<LocaleProp, String> map =
                    new LinkedHashMap<LocaleProp, String>(
                        (Map) super.translations());
                for (Map.Entry<LocaleProp, String> entry : map.entrySet()) {
                    entry.setValue(name + "." + entry.getValue());
                }
                //noinspection unchecked
                return (Map) map;
            }

            @Override
            public List<Object> getElements() {
                final List<Object> list =
                    new ArrayList<Object>(this.larder.getElements());
                for (int i = 0; i < list.size(); i++) {
                    Object o = list.get(i++);
                    if (o instanceof LocaleProp) {
                        LocaleProp key = (LocaleProp) o;
                        list.set(i, get(key.right, key.left));
                    }
                }
                return list;
            }
        };
    }

    /** Returns the caption of a named element, using the caption in the larder
     * first, and calling back to the element's name. */
    public static String getCaption(Named named, Larder larder) {
        final String caption =
            larder.get(LocalizedProperty.CAPTION, DEFAULT_LOCALE);
        if (caption != null) {
            return caption;
        }
        return named.getName();
    }

    /** Returns the description of an element from a larder. */
    public static String getDescription(Larder larder) {
        return larder.get(LocalizedProperty.DESCRIPTION, DEFAULT_LOCALE);
    }

    public static Larder set(Larder larder, Property property, Object value) {
        return new LarderBuilder()
            .populate((LarderImpl) larder)
            .add(property, value)
            .build();
    }

    /** Whether a locale is a child of another. For example, "fr_CA" is
     * a child of "fr". Thus a resource for "fr" could satisfy a client
     * in locale "fr_CA" (albeit not as well as a resource for "fr_CA"). */
    private static boolean isChild(Locale child, Locale parent) {
        // Not very efficient, not very correct.
        return parent.toString().startsWith(child.toString());
    }

    public static Larder ofName(String name) {
        return new LarderBuilder().add(Property.NAME, name).build();
    }

    private enum EmptyLarder implements LarderImpl {
        INSTANCE;

        public Map<String, Annotation> getAnnotationMap() {
            return Collections.emptyMap();
        }

        public Map<Pair<Locale, LocalizedProperty>, String> translations() {
            return Collections.emptyMap();
        }

        public String get(LocalizedProperty prop, Locale locale) {
            return null;
        }

        public Object get(Property property) {
            return null;
        }

        public List<Object> getElements() {
            return Collections.emptyList();
        }
    }

    private static class ArrayLarder implements LarderImpl {
        private final Object[] elements;

        // constructor must be private... does not copy array
        private ArrayLarder(Object[] elements) {
            this.elements = elements;
        }

        public List<Object> getElements() {
            return Arrays.asList(elements);
        }

        public Map<String, Annotation> getAnnotationMap() {
            final Map<String, Annotation> map =
                new LinkedHashMap<String, Annotation>();
            for (int i = 0; i < elements.length;) {
                Object element = elements[i++];
                if (element instanceof String) {
                    final String key = (String) element;
                    final Object value = elements[i++];
                    map.put(key, new AnnotationImpl(key, value));
                } else {
                    ++i;
                }
            }
            if (map.isEmpty()) {
                return Collections.emptyMap();
            }
            return map;
        }

        public Map<Pair<Locale, LocalizedProperty>, String> translations() {
            final Map<Pair<Locale, LocalizedProperty>, String> map =
                new HashMap<Pair<Locale, LocalizedProperty>, String>();
            for (int i = 0; i < elements.length;) {
                Object element = elements[i++];
                if (element instanceof LocaleProp) {
                    final LocaleProp key = (LocaleProp) element;
                    map.put(key, (String) elements[i++]);
                } else {
                    ++i;
                }
            }
            if (map.isEmpty()) {
                return Collections.emptyMap();
            }
            return map;
        }

        public String get(LocalizedProperty prop, Locale locale) {
            // REVIEW: we could sort the elements on creation, putting child
            // locales before parent locales, so we know that the first locale
            // that matches is the best one.
            Locale bestLocale = null;
            String bestValue = null;
            for (int i = 0; i < elements.length;) {
                Object element = elements[i++];
                if (element instanceof LocaleProp) {
                    final LocaleProp key = (LocaleProp) element;
                    String value = (String) elements[i++];
                    if (key.right == prop) {
                        if (key.left.equals(locale)) {
                            return value;
                        } else if (isChild(key.left, locale)) {
                            if (bestLocale == null
                                || isChild(bestLocale, key.left))
                            {
                                bestLocale = key.left;
                                bestValue = value;
                            }
                        }
                    }
                } else {
                    ++i;
                }
            }
            return bestValue;
        }

        public Object get(Property property) {
            for (int i = 0; i < elements.length; i++) {
                Object element = elements[i++];
                if (element == property) {
                    return elements[i];
                }
            }
            return null;
        }
    }

    /** Larder that contains a caption and no description or annotations. */
    private static class CaptionLarder implements LarderImpl {
        private final String caption;

        public CaptionLarder(String caption) {
            assert caption != null;
            this.caption = caption;
        }

        public Map<String, Annotation> getAnnotationMap() {
            return Collections.emptyMap();
        }

        public Map<Pair<Locale, LocalizedProperty>, String> translations() {
            return Collections.singletonMap(
                (Pair<Locale, LocalizedProperty>) DEFAULT_LOCALE_CAPTION,
                caption);
        }

        public String get(LocalizedProperty prop, Locale locale) {
            if (prop == LocalizedProperty.CAPTION
                && locale.equals(DEFAULT_LOCALE))
            {
                return caption;
            } else {
                return null;
            }
        }

        public List<Object> getElements() {
            return Util.<Object>flatList(DEFAULT_LOCALE_CAPTION, caption);
        }

        public Object get(Property property) {
            return null;
        }
    }

    private static class DelegatingLarder implements LarderImpl {
        protected final LarderImpl larder;

        DelegatingLarder(LarderImpl larder) {
            this.larder = larder;
        }

        public Map<String, Annotation> getAnnotationMap() {
            return larder.getAnnotationMap();
        }

        public Map<Pair<Locale, LocalizedProperty>, String> translations() {
            return larder.translations();
        }

        public String get(LocalizedProperty prop, Locale locale) {
            return larder.get(prop, locale);
        }

        public Object get(Property property) {
            return larder.get(property);
        }

        public List<Object> getElements() {
            return larder.getElements();
        }
    }

    private static class AnnotationImpl implements Annotation {
        private final String name;
        private final Object value;

        AnnotationImpl(String name, Object value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public Object getValue() {
            return value;
        }
    }

    private static class LocaleProp extends Pair<Locale, LocalizedProperty> {
        private static final List<LocaleProp> INSTANCES =
            new ArrayList<LocaleProp>();

        LocaleProp(Locale left, LocalizedProperty right) {
            super(left, right);
        }

        /** Looks up a canonical locale/property pair. It saves memory
         * because we don't have lots of locale objects. */
        private static synchronized LocaleProp lookup(
            String localeName,
            LocalizedProperty prop)
        {
            for (LocaleProp localeProp : INSTANCES) {
                if (localeProp.right == prop
                    && localeProp.left.toString().equals(localeName))
                {
                    return localeProp;
                }
            }
            final LocaleProp localeProp =
                new LocaleProp(Util.parseLocale(localeName), prop);
            INSTANCES.add(localeProp);
            return localeProp;
        }

        private static synchronized LocaleProp lookup(
            Locale locale,
            LocalizedProperty prop)
        {
            for (LocaleProp localeProp : INSTANCES) {
                if (localeProp.right == prop
                    && localeProp.left.equals(locale))
                {
                    return localeProp;
                }
            }
            final LocaleProp localeProp = new LocaleProp(locale, prop);
            INSTANCES.add(localeProp);
            return localeProp;
        }
    }

    public static class Resource {
        public final LocalizedProperty prop;
        public final Locale locale;
        public final String value;

        public Resource(LocalizedProperty prop, Locale locale, String value) {
            this.prop = prop;
            this.locale = locale;
            this.value = value;
        }

        /** Looks up, in a list of resources, a resource that matches a given
         * property and most closely matches a given resource. */
        public static String lookup(
            LocalizedProperty prop,
            Locale locale,
            List<Resource> resources)
        {
            Locale bestLocale = null;
            String bestValue = null;
            for (Resource resource : resources) {
                    String value = resource.value;
                    if (resource.prop == prop) {
                        if (resource.locale.equals(locale)) {
                            return resource.value;
                        } else if (isChild(resource.locale, locale)) {
                            if (bestLocale == null
                                || isChild(bestLocale, resource.locale))
                            {
                                bestLocale = resource.locale;
                                bestValue = value;
                            }
                        }
                    }
            }
            return bestValue;
        }
    }

    public static class LarderBuilder {
        final List<Object> elements = new ArrayList<Object>();
        final Set<Object> keys = new HashSet<Object>();

        public LarderBuilder() {
        }

        public static LarderBuilder of(Larder larder) {
            return new LarderBuilder().populate(larder);
        }

        public LarderBuilder add(Resource resource) {
            LocaleProp localeProp =
                LocaleProp.lookup(resource.locale, resource.prop);
            if (keys.add(localeProp)) {
                elements.add(localeProp);
                elements.add(resource.value);
            }
            return this;
        }

        public LarderBuilder addAll(List<Resource> resources) {
            if (resources != null) {
                for (Resource resource : resources) {
                    add(
                        LocaleProp.lookup(resource.locale, resource.prop),
                        resource.value);
                }
            }
            return this;
        }

        public LarderBuilder add(MondrianDef.Annotation xmlAnnotation) {
            if (xmlAnnotation.name.startsWith("caption.")) {
                add(
                    LocaleProp.lookup(
                        xmlAnnotation.name.substring("caption.".length()),
                        LocalizedProperty.CAPTION),
                    xmlAnnotation.cdata);
            } else if (xmlAnnotation.name.startsWith("description.")) {
                add(
                    LocaleProp.lookup(
                        xmlAnnotation.name.substring("description.".length()),
                        LocalizedProperty.DESCRIPTION),
                    xmlAnnotation.cdata);
            } else {
                add(xmlAnnotation.name, xmlAnnotation.cdata);
            }
            return this;
        }

        public LarderBuilder addAll(
            NamedList<MondrianDef.Annotation> annotations)
        {
            for (MondrianDef.Annotation annotation : annotations) {
                add(annotation);
            }
            return this;
        }

        void add(String name, Object value) {
            if (keys.add(name)) {
                elements.add(name);
                elements.add(value);
            }
        }

        void add(Annotation annotation) {
            add(annotation.getName(), annotation.getValue());
        }

        public Larder build() {
            switch (elements.size()) {
            case 0:
                return EMPTY;
            case 2:
                final Object o = elements.get(0);
                if (o == DEFAULT_LOCALE_CAPTION) {
                    return ofCaption((String) elements.get(1));
                }
                // fall through
            default:
                return new ArrayLarder(elements.toArray());
            }
        }

        public LarderBuilder caption(String caption) {
            if (caption != null) {
                add(DEFAULT_LOCALE_CAPTION, caption);
            }
            return this;
        }

        public LarderBuilder description(String description) {
            if (description != null) {
                add(DEFAULT_LOCALE_DESCRIPTION, description);
            }
            return this;
        }

        public LarderBuilder name(String name) {
            assert name != null; // use RolapUtil.mdxNullLiteral()
            add(Property.NAME, name);
            return this;
        }

        public LarderBuilder add(LocaleProp localeProp, String value) {
            if (keys.add(localeProp)) {
                elements.add(localeProp);
                elements.add(value);
            }
            return this;
        }

        public final LarderBuilder populate(Larder larder) {
            return populate((LarderImpl) larder);
        }

        LarderBuilder populate(LarderImpl larder) {
            if (elements.isEmpty()) {
                elements.addAll(larder.getElements());
                for (int i = 0; i < elements.size(); i += 2) {
                    keys.add(elements.get(i));
                }
            } else {
                final List<Object> elements1 = larder.getElements();
                for (int i = 0; i < elements1.size(); i++) {
                    Object key = elements1.get(i++);
                    if (keys.add(key)) {
                        elements.add(key);
                        elements.add(elements1.get(i));
                    }
                }
            }
            return this;
        }

        public LarderBuilder add(Property property, Object value) {
            if (keys.add(property)) {
                elements.add(property);
                elements.add(value);
            } else {
                int x = find(elements, property);
                elements.set(x + 1, value);
            }
            return this;
        }

        private int find(List<Object> elements, Object key) {
            for (int i = 0; i < elements.size(); i += 2) {
                if (elements.get(i).equals(key)) {
                    return i;
                }
            }
            return -1;
        }
    }

    private interface LarderImpl extends Larder {
        List<Object> getElements();
    }
}

// End Larders.java
