/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2010 Pentaho
// All Rights Reserved.
//
// jhyde, May 2, 2003
*/

package mondrian.xmla;

import org.olap4j.impl.UnmodifiableArrayMap;
import org.olap4j.metadata.XmlaConstant;
import org.olap4j.metadata.XmlaConstants;

import java.util.List;
import java.util.Map;

/**
 * Contains inner classes which define enumerations used in XML for Analysis.
 *
 * @author jhyde
 * @since May 2, 2003
 */
public class Enumeration {
    public final String name;
    public final String description;
    public final RowsetDefinition.Type type;
    private final XmlaConstant.Dictionary<?> dictionary;

    public static final Enumeration TREE_OP =
        new Enumeration(
            "TREE_OP",
            "Bitmap which controls which relatives of a member are "
            + "returned",
            RowsetDefinition.Type.Integer,
            org.olap4j.metadata.Member.TreeOp.getDictionary());

    public static final Enumeration VISUAL_MODE =
        new Enumeration(
            "VisualMode",
            "This property determines the default behavior for visual "
            + "totals.",
            RowsetDefinition.Type.Integer,
            org.olap4j.metadata.XmlaConstants.VisualMode.getDictionary());

    public static final Enumeration METHODS =
        new Enumeration(
            "Methods",
            "Set of methods for which a property is applicable",
            RowsetDefinition.Type.Enumeration,
            XmlaConstants.Method.getDictionary());

    public static final Enumeration ACCESS =
        new Enumeration(
            "Access",
            "The read/write behavior of a property",
            RowsetDefinition.Type.Enumeration,
            XmlaConstants.Access.getDictionary());

    public static final Enumeration AUTHENTICATION_MODE =
        new Enumeration(
            "AuthenticationMode",
            "Specification of what type of security mode the data source "
            + "uses.",
            RowsetDefinition.Type.EnumString,
            XmlaConstants.AuthenticationMode.getDictionary());

    public static final Enumeration PROVIDER_TYPE =
        new Enumeration(
            "ProviderType",
            "The types of data supported by the provider.",
            RowsetDefinition.Type.Array,
            XmlaConstants.ProviderType.getDictionary());

    public Enumeration(
        String name,
        String description,
        RowsetDefinition.Type type,
        XmlaConstant.Dictionary<?> dictionary)
    {
        this.name = name;
        this.description = description;
        this.type = type;
        this.dictionary = dictionary;
    }

    public String getName() {
        return name;
    }

    public List<? extends Enum> getValues() {
        return dictionary.getValues();
    }

    public enum ResponseMimeType {
        SOAP("text/xml"),
        JSON("application/json");

        public static final Map<String, ResponseMimeType> MAP =
            UnmodifiableArrayMap.of(
                "application/soap+xml", SOAP,
                "application/xml", SOAP,
                "text/xml", SOAP,
                "application/json", JSON,
                "*/*", SOAP);

        private final String mimeType;

        ResponseMimeType(String mimeType) {
            this.mimeType = mimeType;
        }

        public String getMimeType() {
            return mimeType;
        }
    }

}

// End Enumeration.java
