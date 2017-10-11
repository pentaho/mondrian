/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2016-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.format;

import mondrian.resource.MondrianResource;
import mondrian.spi.CellFormatter;
import mondrian.spi.MemberFormatter;
import mondrian.spi.PropertyFormatter;
import mondrian.spi.impl.Scripts;

import java.lang.reflect.Constructor;

/**
 * Formatter factory to provide a single point
 * to create different formatters for element values.
 *
 * <p/>
 * Uses provided context data to instantiate a formatter for the element
 * either by specified class name or script.
 */
public class FormatterFactory {

    /**
     * The default formatter which is used
     * when no custom formatter is specified.
     */
    private static final DefaultFormatter DEFAULT_FORMATTER =
        new DefaultFormatter();


    private static final PropertyFormatter DEFAULT_PROPERTY_FORMATTER =
        new PropertyFormatterAdapter(DEFAULT_FORMATTER);

    private static final MemberFormatter DEFAULT_MEMBER_FORMATTER =
        new DefaultRolapMemberFormatter(DEFAULT_FORMATTER);


    private static final FormatterFactory INSTANCE = new FormatterFactory();

    private FormatterFactory() {
    }

    public static FormatterFactory instance() {
        return INSTANCE;
    }

    /**
     * Given the name of a cell formatter class and/or a cell formatter script,
     * returns a cell formatter.
     * <p>
     *     Returns null if empty context is passed.
     * </p>
     */
    public CellFormatter createCellFormatter(FormatterCreateContext context) {
        try {
            if (context.getFormatterClassName() != null) {
                return createFormatter(context.getFormatterClassName());
            }
            if (context.getScriptText() != null) {
                return Scripts.cellFormatter(
                    context.getScriptText(),
                    context.getScriptLanguage());
            }
        } catch (Exception e) {
            throw MondrianResource.instance().CellFormatterLoadFailed.ex(
                context.getFormatterClassName(),
                context.getElementName(),
                e);
        }
        return null;
    }

    /**
     * Given the name of a member formatter class
     * and/or a member formatter script, returns a member formatter.
     * <p>
     *     Returns default formatter implementation
     *     if empty context is passed.
     * </p>
     */
    public MemberFormatter createRolapMemberFormatter(
        FormatterCreateContext context)
    {
        try {
            if (context.getFormatterClassName() != null) {
                return createFormatter(context.getFormatterClassName());
            }
            if (context.getScriptText() != null) {
                return Scripts.memberFormatter(
                    context.getScriptText(),
                    context.getScriptLanguage());
            }
        } catch (Exception e) {
            throw MondrianResource.instance().MemberFormatterLoadFailed.ex(
                context.getFormatterClassName(),
                context.getElementName(),
                e);
        }
        return DEFAULT_MEMBER_FORMATTER;
    }

    /**
     * Given the name of a property formatter class
     * and/or a property formatter script,
     * returns a property formatter.
     * <p>
     *     Returns default formatter implementation
     *     if empty context is passed.
     * </p>
     */
    public PropertyFormatter createPropertyFormatter(
        FormatterCreateContext context)
    {
        try {
            if (context.getFormatterClassName() != null) {
                return createFormatter(context.getFormatterClassName());
            }
            if (context.getScriptText() != null) {
                return Scripts.propertyFormatter(
                    context.getScriptText(),
                    context.getScriptLanguage());
            }
        } catch (Exception e) {
            throw MondrianResource.instance().PropertyFormatterLoadFailed.ex(
                context.getFormatterClassName(),
                context.getElementName(),
                e);
        }
        return DEFAULT_PROPERTY_FORMATTER;
    }

    private static <T> T createFormatter(String className) throws Exception {
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) Class.forName(className);
        Constructor<T> constructor = clazz.getConstructor();
        return constructor.newInstance();
    }
}
// End FormatterFactory.java