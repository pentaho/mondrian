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

import org.olap4j.impl.Named;

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
        String caption,
        String description)
    {
        return create(
            Collections.<MondrianDef.Annotation>emptyList(),
            caption,
            description);
    }

    public static Larder create(
        List<MondrianDef.Annotation> annotations,
        String caption,
        String description)
    {
        int n = 0;
        if (annotations == null) {
            annotations = Collections.emptyList();
        }
        n += annotations.size() * 2;
        if (caption != null) {
            n += 2;
        }
        if (description != null) {
            n += 2;
        }
        if (n == 0) {
            return EmptyLarder.INSTANCE;
        }
        if (n == 2 && caption != null) {
            return ofCaption(caption);
        }
        Object[] elements = new Object[n];
        int i = 0;
        for (MondrianDef.Annotation annotation : annotations) {
            if (annotation.name.startsWith("caption.")) {
                elements[i++] = LocaleProp.lookup(
                    annotation.name.substring("caption.".length()),
                    LocalizedProperty.CAPTION);
            } else if (annotation.name.startsWith("description.")) {
                elements[i++] = LocaleProp.lookup(
                    annotation.name.substring("description.".length()),
                    LocalizedProperty.DESCRIPTION);
            } else {
                elements[i++] = annotation.name;
            }
            elements[i++] = annotation.cdata;
        }
        if (caption != null) {
            elements[i++] = DEFAULT_LOCALE_CAPTION;
            elements[i++] = caption;
        }
        if (description != null) {
            elements[i++] = DEFAULT_LOCALE_DESCRIPTION;
            elements[i++] = description;
        }
        assert i == n;
        return new ArrayLarder(elements);
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

    /** Returns a Larder that contains a caption and nothing else. */
    public static Larder ofCaption(String caption) {
        if (caption == null) {
            return EmptyLarder.INSTANCE;
        } else {
            return new CaptionLarder(caption);
        }
    }

    /** Returns a Larder that gets map and description from a property
     * map. */
    public static Larder ofProperties(Map<String, Object> map) {
        return new PropertiesLarder(map);
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
        return new DelegatingLarder(larder) {
            public String get(LocalizedProperty prop, Locale locale) {
                final String value = super.get(prop, locale);
                if (value != null) {
                    return name + "." + value;
                }
                return value;
            }

            @Override
            public Map<Pair<Locale, LocalizedProperty>, String> translations() {
                final Map<Pair<Locale, LocalizedProperty>, String> map =
                    new LinkedHashMap<Pair<Locale, LocalizedProperty>, String>(
                        super.translations());
                for (Map.Entry<Pair<Locale, LocalizedProperty>, String> entry
                    : map.entrySet())
                {
                    entry.setValue(name + "." + entry.getValue());
                }
                return map;
            }
        };
    }

    /** Returns a larder the same as one provided, except that caption and
     * description override, if set. */
    public static Larder override(
        Larder larder,
        String caption,
        String description)
    {
        if (caption != null) {
            larder =
                replace(
                    larder, LocalizedProperty.CAPTION,
                    DEFAULT_LOCALE, caption);
        }
        if (description != null) {
            larder =
                replace(
                    larder, LocalizedProperty.DESCRIPTION,
                    DEFAULT_LOCALE, description);
        }
        return larder;
    }

    /** Replaces or adds a (locale, property) combination to a larder. Since
     * larders are immutable, creates a new larder if changes are needed. */
    private static Larder replace(
        Larder larder,
        LocalizedProperty prop,
        Locale locale,
        String value)
    {
        assert value != null;
        final String previous = larder.get(prop, locale);
        if (Util.equals(previous, value)) {
            return larder;
        }
        final List<Object> list = new ArrayList<Object>();
        for (Annotation annotation : larder.getAnnotationMap().values()) {
            list.add(annotation.getName());
            list.add(annotation.getValue());
        }
        final LocaleProp key = LocaleProp.lookup(locale, prop);
        int match = 0;
        for (Map.Entry<Pair<Locale, LocalizedProperty>, String> entry
            : larder.translations().entrySet())
        {
            assert entry.getKey() instanceof LocaleProp;
            list.add(entry.getKey());
            if (entry.getKey().equals(key)) {
                list.add(value);
                ++match;
            } else {
                list.add(entry.getValue());
            }
        }
        if (match == 0) {
            list.add(key);
            list.add(value);
        }
        return new ArrayLarder(list.toArray());
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

    private enum EmptyLarder implements Larder {
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
    }

    private static class ArrayLarder implements Larder {
        private final Object[] elements;

        // must be private... does not copy array
        private ArrayLarder(Object[] elements) {
            this.elements = elements;
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
                if (element instanceof String) {
                    ++i;
                } else {
                    final LocaleProp key = (LocaleProp) element;
                    map.put(key, (String) elements[i++]);
                }
            }
            if (map.isEmpty()) {
                return Collections.emptyMap();
            }
            return map;
        }

        public String get(LocalizedProperty prop, Locale locale) {
            Locale bestLocale = null;
            String bestValue = null;
            for (int i = 0; i < elements.length;) {
                Object element = elements[i++];
                if (element instanceof String) {
                    ++i;
                } else {
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
                }
            }
            return bestValue;
        }

        /** Whether a locale is a child of another. For example, "fr_CA" is
         * a child of "fr". Thus a resource for "fr" could satisfy a client
         * in locale "fr_CA" (albeit not as well as a resource for "fr_CA"). */
        private static boolean isChild(Locale child, Locale parent) {
            // Not very efficient, not very correct.
            return parent.toString().startsWith(child.toString());
        }
    }

    /** Larder that contains a caption and no description or annotations. */
    private static class CaptionLarder implements Larder {
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
            if (prop == LocalizedProperty.CAPTION && locale == DEFAULT_LOCALE) {
                return caption;
            } else {
                return null;
            }
        }
    }

    /** Larder that gets caption and description from a map. */
    private static class PropertiesLarder implements Larder {
        private final Map<String, Object> map;

        PropertiesLarder(Map<String, Object> map) {
            this.map = map;
        }

        public Map<String, Annotation> getAnnotationMap() {
            return Collections.emptyMap();
        }

        public Map<Pair<Locale, LocalizedProperty>, String> translations() {
            final String c = (String) map.get(Property.CAPTION.name);
            final String d = (String) map.get(Property.DESCRIPTION.name);
            if (c == null && d == null) {
                return Collections.emptyMap();
            }
            final Map<Pair<Locale, LocalizedProperty>, String> map1 =
                new HashMap<Pair<Locale, LocalizedProperty>, String>();
            if (c != null) {
                map1.put(DEFAULT_LOCALE_CAPTION, c);
            }
            if (d != null) {
                map1.put(DEFAULT_LOCALE_DESCRIPTION, d);
            }
            return map1;
        }

        public String get(LocalizedProperty prop, Locale locale) {
            if (locale != DEFAULT_LOCALE) {
                return null;
            }
            switch (prop) {
            case CAPTION:
                return (String) map.get(Property.CAPTION.name);
            case DESCRIPTION:
                return (String) map.get(Property.DESCRIPTION.name);
            default:
                return null;
            }
        }
    }

    private static class DelegatingLarder implements Larder {
        private final Larder larder;

        DelegatingLarder(Larder larder) {
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
}

// End Larders.java
