/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.olap;

import org.apache.logging.log4j.Logger;

import java.util.Locale;
import java.util.Map;

/**
 * <code>OlapElementBase</code> is an abstract base class for implementations of
 * {@link OlapElement}.
 *
 * @author jhyde
 * @since 6 August, 2001
 */
public abstract class OlapElementBase
    implements OlapElement
{
    protected String caption = null;

    protected boolean visible = true;

    // cache hash-code because it is often used and elements are immutable
    private int hash;

    protected OlapElementBase() {
    }

    protected abstract Logger getLogger();

    public boolean equals(Object o) {
        return (o == this)
           || ((o instanceof OlapElement)
               && equals((OlapElement) o));
    }

    public boolean equals(OlapElement mdxElement) {
        return mdxElement != null
           && getClass() == mdxElement.getClass()
           && getUniqueName().equalsIgnoreCase(mdxElement.getUniqueName());
    }

    public int hashCode() {
        if (hash == 0) {
            hash = computeHashCode();
        }
        return hash;
    }

    /**
     * Computes this object's hash code. Called at most once.
     *
     * @return hash code
     */
    protected int computeHashCode() {
        return (getClass().hashCode() << 8) ^ getUniqueName().hashCode();
    }

    public String toString() {
        return getUniqueName();
    }

    public Object clone() {
        return this;
    }

    /**
     * Returns the display name of this catalog element.
     * If no caption is defined, the name is returned.
     */
    public String getCaption() {
        if (caption != null) {
            return caption;
        } else {
            return getName();
        }
    }

    /**
     * Sets the display name of this catalog element.
     */
    public void setCaption(String caption) {
        this.caption = caption;
    }

    public boolean isVisible() {
        return visible;
    }

    public String getLocalized(LocalizedProperty prop, Locale locale) {
        if (this instanceof Annotated) {
            Annotated annotated = (Annotated) this;
            final Map<String, Annotation> annotationMap =
                annotated.getAnnotationMap();
            if (!annotationMap.isEmpty()) {
                String seek = prop.name().toLowerCase() + "." + locale;
                for (;;) {
                    for (Map.Entry<String, Annotation> entry
                        : annotationMap.entrySet())
                    {
                        if (entry.getKey().startsWith(seek)) {
                            return entry.getValue().getValue().toString();
                        }
                    }

                    // No match for locale. Is there a match for the parent
                    // locale? For example, we've just looked for
                    // 'caption.en_US', now look for 'caption.en'.
                    final int underscore = seek.lastIndexOf('_');
                    if (underscore < 0) {
                        break;
                    }
                    seek = seek.substring(0, underscore - 1);
                }
            }
        }

        // No annotation. Fall back to the default caption/description.
        switch (prop) {
        case CAPTION:
            return getCaption();
        case DESCRIPTION:
            return getDescription();
        default:
            throw Util.unexpected(prop);
        }
    }
}

// End OlapElementBase.java
