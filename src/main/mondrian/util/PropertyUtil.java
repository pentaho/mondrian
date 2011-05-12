/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
// Copyright (C) 2011-2011 Julian Hyde
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.MondrianProperties;
import org.eigenbase.util.property.BooleanProperty;
import org.eigenbase.util.property.DoubleProperty;
import org.eigenbase.util.property.IntegerProperty;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Utilities to generate MondrianProperties.java and mondrian.properties
 * from property definitions in MondrianProperties.xml.
 *
 * @version $Id$
 * @author jhyde
 */
public class PropertyUtil {
    /**
     * Generates an XML file from a MondrinProperties instance.
     *
     * @param args Arguments
     * @throws IllegalAccessException on error
     */
    public static void main0(String[] args) throws IllegalAccessException {
        MondrianProperties properties1 = MondrianProperties.instance();
        System.out.println("<PropertyDefinitions>");
        for (Field field : properties1.getClass().getFields()) {
            org.eigenbase.util.property.Property o =
                (org.eigenbase.util.property.Property) field.get(properties1);
            System.out.println("    <PropertyDefinition>");
            System.out.println("        <Name>" + field.getName() + "</Name>");
            System.out.println("        <Path>" + o.getPath() + "</Path>");
            System.out.println(
                "        <Description>" + o.getPath() + "</Description>");
            System.out.println(
                "        <Type>"
                + (o instanceof BooleanProperty ? "boolean"
                    : o instanceof IntegerProperty ? "int"
                        : o instanceof DoubleProperty ? "double"
                            : "String")
                + "</Type>");
            if (o.getDefaultValue() != null) {
                System.out.println(
                    "        <Default>"
                    + o.getDefaultValue() + "</Default"  + ">");
            }
            System.out.println("    </PropertyDefinition>");
        }
        System.out.println("</PropertyDefinitions>");
    }

    private static Iterable<Node> iter(final NodeList nodeList) {
        return new Iterable<Node>() {
            public Iterator<Node> iterator() {
                return new Iterator<Node>() {
                    int pos = 0;

                    public boolean hasNext() {
                        return pos < nodeList.getLength();
                    }

                    public Node next() {
                        return nodeList.item(pos++);
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Generates MondrianProperties.java from MondrianProperties.xml.
     *
     * @param args Arguments
     * @throws IllegalAccessException on error
     */
    public static void main(String[] args)
        throws IllegalAccessException,
        ParserConfigurationException, IOException, SAXException
    {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        dbf.setExpandEntityReferences(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc =
            db.parse("src/main/mondrian/olap/MondrianProperties.xml");
        Element documentElement = doc.getDocumentElement();
        assert documentElement.getNodeName().equals("PropertyDefinitions");
        NodeList propertyDefinitions =
            documentElement.getChildNodes();
        SortedMap<String, Element> propertyDefinitionMap =
            new TreeMap<String, Element>();
        for (Node element : iter(propertyDefinitions)) {
            if (element.getNodeName().equals("PropertyDefinition")) {
                String name = getChildCdata(element, "Name");
                propertyDefinitionMap.put(name, (Element) element);
            }
        }

        System.out.println("// Generated from MondrianProperties.xml.");
        System.out.println("package mondrian.olap;");
        System.out.println();
        System.out.println("import org.eigenbase.util.property.*;");
        System.out.println();

        printJavadoc(
            "",
            "Configuration properties that determine the\n"
            + "behavior of a mondrian instance.\n"
            + "\n"
            + "<p>There is a method for property valid in a\n"
            + "<code>mondrian.properties</code> file. Although it is possible to retrieve\n"
            + "properties using the inherited {@link java.util.Properties#getProperty(String)}\n"
            + "method, we recommend that you use methods in this class.</p>\n"
            + "\n"
            + "@version $Id$\n");
        System.out.println(
            "public class MondrianProperties extends MondrianPropertiesBase {");
        for (Element element : propertyDefinitionMap.values()) {
            String name = getChildCdata(element, "Name");
            String dflt = getChildCdata(element, "Default");
            String type = getChildCdata(element, "Type");
            String path = getChildCdata(element, "Path");
            String description = getChildCdata(element, "Description");
            printJavadoc("    ", description);
            String className;
            String defaultStr = dflt;
            if (type.equals("int")) {
                className = "IntegerProperty";
            } else if (type.equals("double")) {
                className = "DoubleProperty";
            } else if (type.equals("boolean")) {
                className = "BooleanProperty";
            } else {
                className = "StringProperty";
                if (dflt == null) {
                    defaultStr = "null";
                } else {
                    defaultStr =
                        "\"" + dflt.replaceAll("\"", "\\\"") + "\"";
                }
            }
            System.out.println(
                "    public transient final " + className + " " + name + " "
                + "=");
            System.out.println(
                "        new " + className + "(");
            System.out.println(
                "            this, \"" + path + "\", "
                + "" + defaultStr + ");");
            System.out.println();
        }
        System.out.println("}");
        System.out.println();
        System.out.println("// End MondrianProperties.java");
    }

    private static void printJavadoc(String prefix, String s) {
        System.out.println(prefix + "/**");
        for (String javadocLine : wrapText(s)) {
            if (javadocLine.length() > 0) {
                System.out.println(prefix + " * " + javadocLine);
            } else {
                System.out.println(prefix + " *");
            }
        }
        System.out.println(prefix + " */");
    }

    private static List<String> wrapText(String description) {
        description = description.trim();
        return Arrays.asList(description.split("\n"));
    }

    private static String getChildCdata(Node element, String name) {
        for (Node node : iter(element.getChildNodes())) {
            if (node.getNodeName().equals(name)) {
                StringBuilder buf = new StringBuilder();
                textRecurse(node, buf);
                return buf.toString();
            }
        }
        return null;
    }

    private static void textRecurse(Node node, StringBuilder buf) {
        for (Node node1 : iter(node.getChildNodes())) {
            if (node1.getNodeType() == Node.CDATA_SECTION_NODE
                || node1.getNodeType() == Node.TEXT_NODE)
            {
                buf.append(node1.getTextContent());
            }
            if (node1.getNodeType() == Node.ELEMENT_NODE) {
                buf.append("<").append(node1.getNodeName()).append(">");
                textRecurse(node1, buf);
                buf.append("</").append(node1.getNodeName()).append(">");
            }
        }
    }
}

// End PropertyUtil.java
