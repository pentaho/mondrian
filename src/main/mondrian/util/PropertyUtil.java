/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.pref.Scope;

import org.w3c.dom.*;

import java.io.*;
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
    /*
     * Generates an XML file from a PrefDef instance.
     *
     * @param args Arguments
     * @throws IllegalAccessException on error
     */
    /*
    public static void main0(String[] args) throws IllegalAccessException {
        final PrintStream out = System.out;
        out.println("<PropertyDefinitions>");
        for (BaseProperty o : PrefDef.MAP.values()) {
            out.println("    <PropertyDefinition>");
            out.println("        <Name>" + o.name + "</Name>");
            out.println("        <Path>" + o.path + "</Path>");
            out.println(
                "        <Description>" + o.getPath() + "</Description>");
            out.println(
                "        <Type>"
                + (o instanceof BooleanProperty ? "boolean"
                    : o instanceof IntegerProperty ? "int"
                        : o instanceof DoubleProperty ? "double"
                            : "String")
                + "</Type>");
            if (o.defaultValue != null) {
                out.println(
                    "        <Default>" + o.defaultValue + "</Default>");
            }
            out.println("    </PropertyDefinition>");
        }
        out.println("</PropertyDefinitions>");
    }
    */

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
     * Generates several Java files from MondrianProperties.xml.
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
        final File serverJavaFile =
            new File("src/main/mondrian/pref", "ServerPref.java");
        final File schemaJavaFile =
            new File("src/main/mondrian/pref", "SchemaPref.java");
        final File connectionJavaFile =
            new File("src/main/mondrian/pref", "ConnectionPref.java");
        final File statementJavaFile =
            new File("src/main/mondrian/pref", "StatementPref.java");
        final File defJavaFile =
            new File("src/main/mondrian/pref", "PrefDef.java");
        final File propertiesFile =
            new File("mondrian.properties.template");
        final File htmlFile = new File("doc", "properties.html");

        final SortedMap<String, PropertyDef> propDefs =
            new TreeMap<String, PropertyDef>();
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
            for (Node element : iter(propertyDefinitions)) {
                if (element.getNodeName().equals("PropertyDefinition")) {
                    String name = getChildCdata(element, "Name");
                    String dflt = getChildCdata(element, "Default");
                    String type = getChildCdata(element, "Type");
                    String path = getChildCdata(element, "Path");
                    String category = getChildCdata(element, "Category");
                    String core = getChildCdata(element, "Core");
                    String description = getChildCdata(element, "Description");
                    final String scopeName = getChildCdata(element, "Scope");
                    Scope scope =
                        scopeName == null
                            ? Scope.Statement
                            : Scope.valueOf(scopeName);
                    propDefs.put(
                        name,
                        new PropertyDef(
                            name,
                            path,
                            dflt,
                            category,
                            PropertyType.valueOf(type.toUpperCase()),
                            scope,
                            core == null || Boolean.valueOf(core),
                            description));
                }
            }
        } catch (Throwable e) {
            throw new RuntimeException("Error while parsing " + xmlFile, e);
        }
        doGenerate(Generator.DEF, propDefs, defJavaFile);
        doGenerate(Generator.SERVER, propDefs, serverJavaFile);
        doGenerate(Generator.SCHEMA, propDefs, schemaJavaFile);
        doGenerate(Generator.CONNECTION, propDefs, connectionJavaFile);
        doGenerate(Generator.STATEMENT, propDefs, statementJavaFile);
        doGenerate(Generator.HTML, propDefs, htmlFile);
        doGenerate(Generator.PROPERTIES, propDefs, propertiesFile);
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

    private static void printLines(PrintWriter out, String... lines) {
        for (String line : lines) {
            out.println(line);
        }
    }

    static void generateX(
        SortedMap<String, PropertyDef> propertyDefinitionMap,
        Scope scope,
        PrintWriter out)
    {
        printLines(
            out,
            "",
            "    public void readFrom(Map... maps) {");
        for (PropertyDef def : propertyDefinitionMap.values()) {
            if (!def.core || def.scope != scope) {
                continue;
            }
            out.println(
                "        " + def.name
                + " = Prefs.read(maps, PrefDef." + def.name + ");");
        }
        printLines(
            out,
            "    }");
        for (PropertyDef def : propertyDefinitionMap.values()) {
            if (!def.core || def.scope != scope) {
                continue;
            }
            printJavadoc(
                out, 4, def.description + "\n@see PrefDef#" + def.name);
            out.println(
                "    public "
                    + def.propertyType.fieldClass + " " + def.name + ";");
            out.println();
        }
    }

    enum Generator {
        DEF {
            void generate(
                SortedMap<String, PropertyDef> propertyDefinitionMap,
                File file,
                PrintWriter out)
            {
                printLines(
                    out,
                    "// This class is generated from MondrianProperties.xml.",
                    "package mondrian.pref;",
                    "",
                    "import java.lang.reflect.Field;",
                    "import java.util.*;",
                    "");
                printJavadoc(
                    out,
                    0,
                    "Configuration properties that determine the\n"
                    + "behavior of a mondrian instance.\n"
                    + "\n"
                    + "<p>There is a method for property valid in a\n"
                    + "<code>mondrian.properties</code> file. Although it is possible to retrieve\n"
                    + "properties using the inherited {@link java.util.Properties#getProperty(String)}\n"
                    + "method, we recommend that you use methods in this class.</p>\n");
                printLines(
                    out,
                    "public class PrefDef {");
                for (PropertyDef def : propertyDefinitionMap.values()) {
                    if (!def.core) {
                        continue;
                    }
                    printJavadoc(out, 4, def.description);
                    out.println(
                        "    public static final "
                        + def.propertyType.className + " " + def.name + " =");
                    out.println(
                        "        new " + def.propertyType.className + "(");
                    out.println(
                        "            \"" + def.name
                        + "\", Scope." + def.scope
                        + ", \"" + def.path + "\", "
                        + "" + def.defaultJava() + ");");
                    out.println();
                }
                printLines(
                    out,
                    "",
                    "    public static final Map<String, BaseProperty> MAP = initMap();",
                    "",
                    "    private static Map<String, BaseProperty> initMap() {",
                    "        final Map<String, BaseProperty> map =",
                    "            new LinkedHashMap<String, BaseProperty>();",
                    "        for (Field field : PrefDef.class.getFields()) {",
                    "            if (BaseProperty.class.isAssignableFrom(field.getType())) {",
                    "                try {",
                    "                    BaseProperty property = (BaseProperty) field.get(null);",
                    "                    map.put(property.getPath(), property);",
                    "                } catch (IllegalAccessException e) {",
                    "                    throw new RuntimeException();",
                    "                }",
                    "            }",
                    "        }",
                    "        return Collections.unmodifiableMap(map);",
                    "    }",
                    "}",
                    "",
                    "// End " + file.getName());
            }
        },

        SERVER {
            void generate(
                SortedMap<String, PropertyDef> propertyDefinitionMap,
                File file,
                PrintWriter out)
            {
                printLines(
                    out,
                    "// This class is generated from MondrianProperties.xml.",
                    "package mondrian.pref;",
                    "",
                    "import java.util.*;",
                    "");
                printJavadoc(out, 0, "Preferences of a Mondrian server.");
                printLines(
                    out,
                    "public class ServerPref {",
                    "    private static final ServerPref INSTANCE = Prefs.initServer();",
                    "",
                    "    int populateCount;",
                    "",
                    "    /** Default values for {@link mondrian.pref.StatementPref} created in this",
                    "     * connection. */",
                    "    public final Map<BaseProperty, Object> defaultMap =",
                    "        new HashMap<BaseProperty, Object>();",
                    "",
                    "    ServerPref() {",
                    "    }",
                    "",
                    "    void populate(Map<String, String> map) {",
                    "        Prefs.load(this, Scope.System, defaultMap, map);",
                    "    }",
                    "",
                    "    public Object get(BaseProperty property) {",
                    "        switch (property.scope) {",
                    "        case System:",
                    "            return Prefs.get_(this, property.name);",
                    "        default:",
                    "            throw new AssertionError(property.scope);",
                    "        }",
                    "    }",
                    "",
                    "    public static ServerPref instance() {",
                    "        return INSTANCE;",
                    "    }");
                generateX(propertyDefinitionMap, Scope.System, out);
                printLines(
                    out,
                    "}",
                    "",
                    "// End " + file.getName());
            }
        },

        SCHEMA {
            void generate(
                SortedMap<String, PropertyDef> propertyDefinitionMap,
                File file,
                PrintWriter out)
            {
                printLines(
                    out,
                    "// This class is generated from MondrianProperties.xml.",
                    "package mondrian.pref;",
                    "",
                    "import java.util.*;",
                    "");
                printJavadoc(out, 0, "Preferences of a Mondrian schema.");
                printLines(
                    out,
                    "public class SchemaPref {",
                    "    public final ServerPref server;",
                    "    public final Map<BaseProperty, Object> defaultMap =",
                    "        new HashMap<BaseProperty, Object>();",
                    "",
                    "    SchemaPref(ServerPref server) {",
                    "        this.server = server;",
                    "        readFrom(server.defaultMap);",
                    "    }",
                    "",
                    "    public Object get(BaseProperty property) {",
                    "        switch (property.scope) {",
                    "        case System:",
                    "            return Prefs.get_(server, property.name);",
                    "        case Schema:",
                    "            return Prefs.get_(this, property.name);",
                    "        default:",
                    "            throw new AssertionError(property.scope);",
                    "        }",
                    "    }");
                generateX(propertyDefinitionMap, Scope.Schema, out);
                printLines(
                    out,
                    "}",
                    "",
                    "// End " + file.getName());
            }
        },

        CONNECTION {
            void generate(
                SortedMap<String, PropertyDef> propertyDefinitionMap,
                File file,
                PrintWriter out)
            {
                printLines(
                    out,
                    "// This class is generated from MondrianProperties.xml.",
                    "package mondrian.pref;",
                    "",
                    "import java.util.*;",
                    "");
                printJavadoc(out, 0, "Preferences of a Mondrian connection.");
                printLines(
                    out,
                    "public class ConnectionPref {",
                    "    public final ServerPref server;",
                    "    public final Map<BaseProperty, Object> defaultMap =",
                    "        new HashMap<BaseProperty, Object>();",
                    "",
                    "    ConnectionPref(ServerPref server) {",
                    "        this.server = server;",
                    "        readFrom(server.defaultMap);",
                    "    }",
                    "",
                    "    public Object get(BaseProperty property) {",
                    "        switch (property.scope) {",
                    "        case System:",
                    "            return Prefs.get_(server, property.name);",
                    "        case Connection:",
                    "            return Prefs.get_(this, property.name);",
                    "        default:",
                    "            throw new AssertionError(property.scope);",
                    "        }",
                    "    }",
                    "",
                    "    public static ConnectionPref instance() {",
                    "        return null;",
                    "    }");
                generateX(propertyDefinitionMap, Scope.Connection, out);
                printLines(
                    out,
                    "}",
                    "",
                    "// End " + file.getName());
            }
        },

        STATEMENT {
            void generate(
                SortedMap<String, PropertyDef> propertyDefinitionMap,
                File file,
                PrintWriter out)
            {
                printLines(
                    out,
                    "// This class is generated from MondrianProperties.xml.",
                    "package mondrian.pref;",
                    "",
                    "import java.util.*;",
                    "");
                printJavadoc(out, 0, "Preferences of a Mondrian statement.");
                printLines(
                    out,
                    "public class StatementPref {",
                    "    public final ServerPref server;",
                    "    public final SchemaPref schema;",
                    "    public final ConnectionPref connection;",
                    "",
                    "    StatementPref(ConnectionPref connection, SchemaPref schema) {",
                    "        this.connection = connection;",
                    "        this.schema = schema;",
                    "        this.server = connection.server;",
                    "        readFrom(connection.defaultMap);",
                    "    }",
                    "",
                    "    public Object get(BaseProperty property) {",
                    "        switch (property.scope) {",
                    "        case System:",
                    "            return Prefs.get_(server, property.name);",
                    "        case Connection:",
                    "            return Prefs.get_(connection, property.name);",
                    "        case Schema:",
                    "            return Prefs.get_(schema, property.name);",
                    "        case Statement:",
                    "            return Prefs.get_(this, property.name);",
                    "        default:",
                    "            throw new AssertionError(property.scope);",
                    "        }",
                    "    }",
                    "",
                    "    public static StatementPref instance() {",
                    "        return mondrian.rolap.RolapConnection.INTERNAL_STATEMENT_PREF; // FIXME\n",
                    "    }");
                generateX(propertyDefinitionMap, Scope.Statement, out);
                printLines(
                    out,
                    "}",
                    "",
                    "// End " + file.getName());
            }
        },

        HTML {
            void generate(
                SortedMap<String, PropertyDef> propertyDefinitionMap,
                File file,
                PrintWriter out)
            {
                out.println("<table>");
                out.println("    <tr>");
                out.println("    <td><strong>Property</strong></td>");
                out.println("    <td><strong>Scope</strong></td>");
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
                        "      <td colspan='5'><b><br>" + category + "</b"
                        + "></td>");
                    out.println("    </tr>");
                    for (PropertyDef def : propertyDefinitionMap.values()) {
                        if (!def.category.equals(category)) {
                            continue;
                        }
                        out.println("    <tr>");
                        out.println(
                            "<td><code><a href='api/mondrian/pref/PrefDef.html#"
                            + def.name + "'>" + split(def.path)
                            + "</a></code></td>");
                        out.println(
                            "<td>" + def.scope.name().toLowerCase()
                            + "</td>");
                        out.println(
                            "<td>" + def.propertyType.name().toLowerCase()
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
        PrintWriter out, int prefixLength, String content)
    {
        final char[] chars = new char[prefixLength];
        Arrays.fill(chars, ' ');
        final String prefix = String.valueOf(chars);
        out.println(prefix + "/**");
        if (!content.endsWith("\n")) {
            content += "\n";
        }
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
        private final Scope scope;
        private final boolean core;
        private final String description;
        private final String path;

        PropertyDef(
            String name,
            String path,
            String defaultValue,
            String category,
            PropertyType propertyType,
            Scope scope,
            boolean core,
            String description)
        {
            this.name = name;
            this.path = path;
            this.defaultValue = defaultValue;
            this.scope = scope;
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
        INT("IntegerProperty", "int"),
        STRING("StringProperty", "String"),
        DOUBLE("DoubleProperty", "double"),
        BOOLEAN("BooleanProperty", "boolean");

        public final String className;
        private final String fieldClass;

        PropertyType(String className, String fieldClass) {
            this.className = className;
            this.fieldClass = fieldClass;
        }
    }
}

// End PropertyUtil.java
