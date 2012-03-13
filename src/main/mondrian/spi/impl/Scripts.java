/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import mondrian.olap.Util;
import mondrian.spi.*;

/**
 * Provides implementations of a variety of SPIs using scripting.
 *
 * @author jhyde
 */
public class Scripts {

    private static <T> T create(
        ScriptDefinition script,
        Class<T> iface,
        String script2)
    {
        final String engineName = script.language.engineName;
        return Util.compileScript(iface, script2, engineName);
    }

    private static String simple(ScriptDefinition script, String decl) {
        switch (script.language) {
        case JAVASCRIPT:
            return "function " + decl + " { " + script.script + " }";
        default:
            throw Util.unexpected(script.language);
        }
    }

    /**
     * Creates an implementation of the {@link PropertyFormatter} SPI based on
     * a script.
     *
     * @param script Script
     * @return property formatter
     */
    public static PropertyFormatter propertyFormatter(
        ScriptDefinition script)
    {
        return create(
            script,
            PropertyFormatter.class,
            simple(
                script,
                "formatProperty(member,propertyName,propertyValue)"));
    }

    /**
     * Creates an implementation of the {@link MemberFormatter} SPI based on
     * a script.
     *
     * @param script Script
     * @return member formatter
     */
    public static MemberFormatter memberFormatter(
        ScriptDefinition script)
    {
        return create(
            script,
            MemberFormatter.class,
            simple(script, "formatMember(member)"));
    }

    /**
     * Creates an implementation of the {@link CellFormatter} SPI based on
     * a script.
     *
     * @param script Script
     * @return cell formatter
     */
    public static CellFormatter cellFormatter(
        ScriptDefinition script)
    {
        return create(
            script,
            CellFormatter.class,
            simple(script, "formatCell(value)"));
    }

    /**
     * Creates an implementation of the {@link DataSourceChangeListener} SPI
     * based on a script.
     *
     * @param script Script
     * @return data source change listener
     */
    public static DataSourceChangeListener dataSourceChangeListener(
        ScriptDefinition script)
    {
        final String code;
        switch (script.language) {
        case JAVASCRIPT:
            code =
                "function isHierarchyChanged(hierarchy) {\n"
                + "  return false;\n"
                + "}\n"
                + "function isAggregationChanged(aggregation) {\n"
                + "  return false;\n"
                + "}\n";
            break;
        default:
            throw Util.unexpected(script.language);
        }
        return create(
            script,
            DataSourceChangeListener.class,
            code);
    }

    /**
     * Creates an implementation of the {@link DataSourceResolver} SPI based on
     * a script.
     *
     * @param script Script
     * @return data source resolver
     */
    public static DataSourceResolver dataSourceResolver(
        ScriptDefinition script)
    {
        return create(
            script,
            DataSourceResolver.class,
            simple(script, "lookup(dataSourceName)"));
    }

    /**
     * Creates an implementation of the {@link DynamicSchemaProcessor} SPI based
     * on a script.
     *
     * @param script Script
     * @return dynamic schema processor
     */
    public static DynamicSchemaProcessor dynamicSchemaProcessor(
        ScriptDefinition script)
    {
        return create(
            script,
            DynamicSchemaProcessor.class,
            simple(script, "processSchema(schemaUrl, connectInfo)"));
    }

    /**
     * Creates an implementation of the {@link UserDefinedFunction} SPI based on
     * a script.
     *
     * <p>The script must declare an object called "obj" that must have a method
     * "evaluate(evaluator, arguments)" and may have fields "name",
     * "description", "syntax", "parameterTypes" and method
     * "getReturnType(parameterTypes)".</p>
     *
     * @param script Script
     * @return user-defined function
     */
    public static UserDefinedFunction userDefinedFunction(
        ScriptDefinition script,
        String name)
    {
        final String code;
        switch (script.language) {
        case JAVASCRIPT:
            code =
                "var mondrian = Packages.mondrian;\n"
                + "function getName() {\n"
                + "  return " + Util.quoteJavaString(name) + ";\n"
                + "}\n"
                + "function getDescription() {\n"
                + "  return this.getName();\n"
                + "}\n"
                + "function getSyntax() {\n"
                + "  return mondrian.olap.Syntax.Function;\n"
                + "}\n"
                + "function getParameterTypes() {\n"
                + "  return new Array();\n"
                + "}\n"
                + "function getReturnType(parameterTypes) {\n"
                + "  return new mondrian.olap.type.ScalarType();\n"
                + "}\n"
                + "function getReservedWords() {\n"
                + "  return null;\n"
                + "}\n"
                + "function execute(evaluator, arguments) {\n"
                + "  return null;\n"
                + "}\n"
                + script.script;
            break;
        default:
            throw Util.unexpected(script.language);
        }
        return create(
            script,
            UserDefinedFunction.class,
            code);
    }

    public static class ScriptDefinition {
        public final String script;
        public final ScriptLanguage language;

        public ScriptDefinition(
            String script,
            ScriptLanguage language)
        {
            this.script = script;
            this.language = language;

            assert script != null;
            assert language != null;
        }
    }

    public enum ScriptLanguage {
        JAVASCRIPT("JavaScript");

        final String engineName;

        ScriptLanguage(String engineName) {
            this.engineName = engineName;
        }

        public static ScriptLanguage lookup(String languageName) {
            for (ScriptLanguage scriptLanguage : values()) {
                if (scriptLanguage.engineName.equals(languageName)) {
                    return scriptLanguage;
                }
            }
            return null;
        }
    }
}

// End Scripts.java
