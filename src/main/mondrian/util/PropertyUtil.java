/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.util;

import org.eigenbase.util.property.*;

import org.w3c.dom.*;

import java.io.*;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Utilities to generate MondrianProperties.java and mondrian.properties
 * from property definitions in MondrianProperties.xml.
 *
 * @author jhyde
 */
public class PropertyUtil {
    /**
     * Generates an XML file from a MondrianProperties instance.
     *
     * @param args Arguments
     * @throws IllegalAccessException on error
     */
    public static void main0(String[] args) throws IllegalAccessException {
        Object properties1 = null; // MondrianProperties.instance();
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
     */
    public static void main(String[] args)
    {
        try {
            new PropertyUtil().generate();
        } catch (Throwable e) {
            System.out.println("Error while generating properties files.");
            e.printStackTrace();
        }
    }

    private void generate() {
        final File xmlFile =
            new File("src/main/mondrian/olap", "MondrianProperties.xml");
        final File javaFile =
            new File("src/main/mondrian/olap", "MondrianProperties.java");
        final File propertiesFile =
            new File("mondrian.properties.template");
        final File htmlFile = new File("doc", "properties.html");

        SortedMap<String, PropertyDef> propertyDefinitionMap;
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setValidating(false);
            dbf.setExpandEntityReferences(false);
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(xmlFile);
            Element documentElement = doc.getDocumentElement();
            assert documentElement.getNodeName().equals("PropertyDefinitions");
            NodeList propertyDefinitions =
                documentElement.getChildNodes();
            propertyDefinitionMap = new TreeMap<String, PropertyDef>();
            for (Node element : iter(propertyDefinitions)) {
                if (element.getNodeName().equals("PropertyDefinition")) {
                    String name = getChildCdata(element, "Name");
                    String dflt = getChildCdata(element, "Default");
                    String type = getChildCdata(element, "Type");
                    String path = getChildCdata(element, "Path");
                    String category = getChildCdata(element, "Category");
                    String core = getChildCdata(element, "Core");
                    String description = getChildCdata(element, "Description");
                    propertyDefinitionMap.put(
                        name,
                        new PropertyDef(
                            name,
                            path,
                            dflt,
                            category,
                            PropertyType.valueOf(type.toUpperCase()),
                            core == null || Boolean.valueOf(core),
                            description));
                }
            }
        } catch (Throwable e) {
            throw new RuntimeException("Error while parsing " + xmlFile, e);
        }
        doGenerate(Generator.JAVA, propertyDefinitionMap, javaFile);
        doGenerate(Generator.HTML, propertyDefinitionMap, htmlFile);
        doGenerate(Generator.PROPERTIES, propertyDefinitionMap, propertiesFile);
    }

    void doGenerate(
        Generator generator,
        SortedMap<String, PropertyDef> propertyDefinitionMap,
        File file)
    {
        FileWriter fw = null;
        PrintWriter out = null;
        boolean success = false;
        try {
            System.out.println("Generating " + file);
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            fw = new FileWriter(file);
            out = new PrintWriter(fw);
            generator.generate(propertyDefinitionMap, file, out);
            out.close();
            fw.close();
            success = true;
        } catch (Throwable e) {
            throw new RuntimeException("Error while generating " + file, e);
        } finally {
            if (out != null) {
                out.close();
            }
            if (fw != null) {
                try {
                    fw.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            if (!success) {
                file.delete();
            }
        }
    }

    private static final void printLines(PrintWriter out, String[] lines) {
        for (String line : lines) {
            out.println(line);
        }
    }

    enum Generator {
        JAVA {
            @Override
            void generate(
                SortedMap<String, PropertyDef> propertyDefinitionMap,
                File file,
                PrintWriter out)
            {
                out.println("// Generated from MondrianProperties.xml.");
                out.println("package mondrian.olap;");
                out.println();
                out.println("import org.eigenbase.util.property.*;");
                out.println("import java.io.File;");
                out.println();

                printJavadoc(
                    out, "",
                    "Configuration properties that determine the\n"
                    + "behavior of a mondrian instance.\n"
                    + "\n"
                    + "<p>There is a method for property valid in a\n"
                    + "<code>mondrian.properties</code> file. Although it is possible to retrieve\n"
                    + "properties using the inherited {@link java.util.Properties#getProperty(String)}\n"
                    + "method, we recommend that you use methods in this class.</p>\n");
                String[] lines = {
                    "public class MondrianProperties extends MondrianPropertiesBase {",
                    "    /**",
                    "     * Properties, drawn from {@link System#getProperties},",
                    "     * plus the contents of \"mondrian.properties\" if it",
                    "     * exists. A singleton.",
                    "     */",
                    "    private static final MondrianProperties instance =",
                    "        new MondrianProperties();",
                    "",
                    "    private MondrianProperties() {",
                    "        super(",
                    "            new FilePropertySource(",
                    "                new File(mondrianDotProperties)));",
                    "        populate();",
                    "    }",
                    "",
                    "    /**",
                    "     * Returns the singleton.",
                    "     *",
                    "     * @return Singleton instance",
                    "     */",
                    "    public static MondrianProperties instance() {",
                    "        // NOTE: We used to instantiate on demand, but",
                    "        // synchronization overhead was significant. See",
                    "        // MONDRIAN-978.",
                    "        return instance;",
                    "    }",
                    "",
                };
                printLines(out, lines);
                for (PropertyDef def : propertyDefinitionMap.values()) {
                    if (!def.core) {
                        continue;
                    }
                    printJavadoc(out, "    ", def.description);
                    out.println(
                        "    public transient final "
                        + def.propertyType.className + " " + def.name + " =");
                    out.println(
                        "        new " + def.propertyType.className + "(");
                    out.println(
                        "            this, \"" + def.path + "\", "
                        + "" + def.defaultJava() + ");");
                    out.println();
                }
                out.println("}");
                out.println();
                out.println("// End MondrianProperties.java");
            }
        },

        HTML {
            @Override
            void generate(
                SortedMap<String, PropertyDef> propertyDefinitionMap,
                File file,
                PrintWriter out)
            {
                out.println("<table>");
                out.println("    <tr>");
                out.println("    <td><strong>Property</strong></td>");
                out.println("    <td><strong>Type</strong></td>");
                out.println("    <td><strong>Default value</strong></td>");
                out.println("    <td><strong>Description</strong></td>");
                out.println("    </tr>");

                SortedSet<String> categories = new TreeSet<String>();
                for (PropertyDef def : propertyDefinitionMap.values()) {
                    categories.add(def.category);
                }
                for (String category : categories) {
                    out.println("    <tr>");
                    out.println(
                        "      <td colspan='4'><b><br>" + category + "</b"
                        + "></td>");
                    out.println("    </tr>");
                    for (PropertyDef def : propertyDefinitionMap.values()) {
                        if (!def.category.equals(category)) {
                            continue;
                        }
                        out.println("    <tr>");
                        out.println(
                            "<td><code><a href='api/mondrian/olap/MondrianProperties.html#"
                            + def.name + "'>" + split(def.path)
                            + "</a></code></td>");
                        out.println(
                            "<td>" + def.propertyType.name() .toLowerCase()
                            + "</td>");
                        out.println(
                            "<td>" + split(def.defaultHtml()) + "</td>");
                        out.println(
                            "<td>" + split(def.description) + "</td>");
                        out.println("    </tr>");
                    }
                }
                out.println("<table>");
            }

            String split(String s) {
                s = s.replaceAll("([,;=.])", "&shy;$1&shy;");
                if (!s.contains("<")) {
                    s  = s.replaceAll("(/)", "&shy;$1&shy;");
                }
                return s;
            }
        },

        PROPERTIES {
            void generate(
                SortedMap<String, PropertyDef> propertyDefinitionMap,
                File file,
                PrintWriter out)
            {
                printComments(
                    out,
                    "",
                    "#",
                    wrapText(
                        "This software is subject to the terms of the Eclipse Public License v1.0\n"
                        + "Agreement, available at the following URL:\n"
                        + "http://www.eclipse.org/legal/epl-v10.html.\n"
                        + "You must accept the terms of that agreement to use this software.\n"
                        + "\n"
                        + "Copyright (C) 2001-2005 Julian Hyde\n"
                        + "Copyright (C) 2005-2011 Pentaho and others\n"
                        + "All Rights Reserved."));
                out.println();

                char[] chars = new char[79];
                Arrays.fill(chars, '#');
                String commentLine = new String(chars);
                for (PropertyDef def : propertyDefinitionMap.values()) {
                    out.println(commentLine);
                    printComments(
                        out, "", "#", wrapText(stripHtml(def.description)));
                    out.println("#");
                    out.println(
                        "#" + def.path + "="
                        + (def.defaultValue == null ? "" : def.defaultValue));
                    out.println();
                }
                printComments(out, "", "#", wrapText("End " + file.getName()));
            }
        };

        abstract void generate(
            SortedMap<String, PropertyDef> propertyDefinitionMap,
            File file,
            PrintWriter out);
    }


    private static void printJavadoc(
        PrintWriter out, String prefix, String content)
    {
        out.println(prefix + "/**");
        printComments(out, prefix, " *", wrapText(content));
        out.println(prefix + " */");
    }

    private static void printComments(
        PrintWriter out,
        String offset,
        String prefix,
        List<String> strings)
    {
        for (String line : strings) {
            if (line.length() > 0) {
                out.println(offset + prefix + " " + line);
            } else {
                out.println(offset + prefix);
            }
        }
    }

    private static String quoteHtml(String s) {
        return s.replaceAll("&", "&amp;")
            .replaceAll(">", "&gt;")
            .replaceAll("<", "&lt;");
    }

    private static String stripHtml(String s) {
        s = s.replaceAll("<li>", "<li>* ");
        s = s.replaceAll("<h3>", "<h3>### ");
        s = s.replaceAll("</h3>", " ###</h3>");
        String[] strings = {
            "p", "code", "br", "ul", "li", "blockquote", "h3", "i" };
        for (String string : strings) {
            s = s.replaceAll("<" + string + "/>", "");
            s = s.replaceAll("<" + string + ">", "");
            s = s.replaceAll("</" + string + ">", "");
        }
        s = replaceRegion(s, "{@code ", "}");
        s = replaceRegion(s, "{@link ", "}");
        s = s.replaceAll("&amp;", "&");
        s = s.replaceAll("&lt;", "<");
        s = s.replaceAll("&gt;", ">");
        return s;
    }

    private static String replaceRegion(String s, String start, String end) {
        int i = 0;
        while ((i = s.indexOf(start, i)) >= 0) {
            int j = s.indexOf(end, i);
            if (j < 0) {
                break;
            }
            s = s.substring(0, i)
                + s.substring(i + start.length(), j)
                + s.substring(j + 1);
            i = j - start.length() - end.length();
        }
        return s;
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
                buf.append(quoteHtml(node1.getTextContent()));
            }
            if (node1.getNodeType() == Node.ELEMENT_NODE) {
                buf.append("<").append(node1.getNodeName()).append(">");
                textRecurse(node1, buf);
                buf.append("</").append(node1.getNodeName()).append(">");
            }
        }
    }

    private static class PropertyDef {
        private final String name;
        private final String defaultValue;
        private final String category;
        private final PropertyType propertyType;
        private final boolean core;
        private final String description;
        private final String path;

        PropertyDef(
            String name,
            String path,
            String defaultValue,
            String category,
            PropertyType propertyType,
            boolean core,
            String description)
        {
            this.name = name;
            this.path = path;
            this.defaultValue = defaultValue;
            this.category = category == null ? "Miscellaneous" : category;
            this.propertyType = propertyType;
            this.core = core;
            this.description = description;
        }

        public String defaultJava() {
            switch (propertyType) {
            case STRING:
                if (defaultValue == null) {
                    return "null";
                } else {
                    return "\"" + defaultValue.replaceAll("\"", "\\\"") + "\"";
                }
            default:
                return defaultValue;
            }
        }

        public String defaultHtml() {
            if (defaultValue == null) {
                return "-";
            }
            switch (propertyType) {
            case INT:
            case DOUBLE:
                return new DecimalFormat("#,###.#").format(
                    new BigDecimal(defaultValue));
            default:
                return defaultValue;
            }
        }
    }

    private enum PropertyType {
        INT("IntegerProperty"),
        STRING("StringProperty"),
        DOUBLE("DoubleProperty"),
        BOOLEAN("BooleanProperty");

        public final String className;

        PropertyType(String className) {
            this.className = className;
        }
    }
}

// End PropertyUtil.java
