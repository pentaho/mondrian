/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 December, 2001 (moved into its own file szuercher 9 February 2004)
*/

package mondrian.resource;

import org.apache.tools.ant.BuildException;

import java.io.*;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.*;

/**
 * Ant task which processes an XML file and generates a C++ or Java class from
 * the resources in it.
 */
class XmlFileTask extends ResourceGen.FileTask {
    String baseClassName;
    String cppBaseClassName;
    private boolean hasInstCon;
    private boolean hasInstThrowCon;
    private boolean hasStringCon;
    private boolean hasStringThrowCon;
    private final HashSet warnedClasses = new HashSet();

    XmlFileTask(ResourceGenTask.Include include, String fileName,
              String className, String baseClassName, boolean outputJava,
              String cppClassName, String cppBaseClassName, boolean outputCpp) {
        this.include = include;
        this.fileName = fileName;
        this.outputJava = outputJava;
        if (className == null) {
            className = Util.fileNameToClassName(fileName, ".xml");
        }
        this.className = className;
        if (baseClassName == null) {
            baseClassName = "mondrian.resource.ShadowResourceBundle";
        }
        this.baseClassName = baseClassName;

        this.outputCpp = outputCpp;
        if (cppClassName == null) {
            cppClassName = Util.fileNameToCppClassName(fileName, ".xml");
        }
        this.cppClassName = cppClassName;
        if (cppBaseClassName == null) {
            cppBaseClassName = "ResourceBundle";
        }
        this.cppBaseClassName = cppBaseClassName;
    }

    void process(ResourceGen generator) throws IOException {
        URL url = Util.convertPathToURL(getFile());
        ResourceDef.ResourceBundle resourceList = Util.load(url);
        if (resourceList.locale == null) {
            throw new BuildException(
                    "Resource file " + url + " must have locale");
        }

        ArrayList localeNames = new ArrayList();
        StringTokenizer tokenizer = new StringTokenizer(include.root.locales,",");
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            localeNames.add(token);
        }

        if (!localeNames.contains(resourceList.locale)) {
            throw new BuildException(
                    "Resource file " + url + " has locale '" +
                    resourceList.locale +
                    "' which is not in the 'locales' list");
        }

        Locale[] locales = new Locale[localeNames.size()];
        for (int i = 0; i < locales.length; i++) {
            String localeName = (String) localeNames.get(i);
            locales[i] = Util.parseLocale(localeName);
            if (locales[i] == null) {
                throw new BuildException(
                        "Invalid locale " + localeName);
            }
        }


        if (outputJava) {
            generateJava(generator, resourceList, null);
        }

        generateProperties(generator, resourceList, null);

        for (int i = 0; i < locales.length; i++) {
            Locale locale = locales[i];
            if (outputJava) {
                generateJava(generator, resourceList, locale);
            }
            generateProperties(generator, resourceList, locale);
        }

        if (outputCpp) {
            generateCpp(generator, resourceList);
        }
    }

    /**
     * Generates a class containing a line for each resource.
     */
    protected void generateBaseJava(
            ResourceGen generator,
            ResourceDef.ResourceBundle resourceList, PrintWriter pw) {
        generateHeader(pw);
        pw.print("public class " + getClassNameSansPackage(null));
        final String baseClass = baseClassName;
        if (baseClass != null) {
            pw.print(" extends " + baseClass);
        }
        String className = getClassNameSansPackage(null);
        pw.println(" {");
        pw.println("    public " + className + "() throws IOException {");
        pw.println("    }");
        pw.println("    private static String baseName = " + Util.quoteForJava(getClassName(null)) + ";");
        pw.println("    /**");
        pw.println("     * Retrieves the singleton instance of {@link " + className + "}. If");
        pw.println("     * the application has called {@link #setThreadLocale}, returns the");
        pw.println("     * resource for the thread's locale.");
        pw.println("     */");
        pw.println("    public static synchronized " + className + " instance() {");
        pw.println("        return (" + className + ") instance(baseName);");
        pw.println("    }");
        pw.println("    /**");
        pw.println("     * Retrieves the instance of {@link " + className + "} for the given locale.");
        pw.println("     */");
        pw.println("    public static synchronized " + className + " instance(Locale locale) {");
        pw.println("        return (" + className + ") instance(baseName, locale);");
        pw.println("    }");
        if (resourceList.code != null) {
            pw.println("\t// begin of included code");
            pw.print(resourceList.code.cdata);
            pw.println("\t// end of included code");
        }

        for (int j = 0; j < resourceList.resources.length; j++) {
            ResourceDef.Resource resource = resourceList.resources[j];
            if (resource.text == null) {
                throw new BuildException(
                        "Resource '" + resource.name + "' has no message");
            }
            String text = resource.text.cdata;
            String comment = ResourceGen.getComment(resource);
            final String resourceInitcap = ResourceGen.getResourceInitcap(resource);// e.g. "Internal"

            String definitionClass = "mondrian.resource.ResourceDefinition";
            // e.g. "mondrian.resource.ChainableRuntimeException"
            String parameterList = ResourceGen.getParameterList(text, true);
            String argumentList = ResourceGen.getArgumentList(text, true);

      generateCommentBlock(pw, resource.name, text, comment);

            pw.println("    public static final " + definitionClass + " " + resourceInitcap + " = new " + definitionClass + "(\"" + resourceInitcap + "\", " + Util.quoteForJava(text) + ");");
            pw.println("    public String get" + resourceInitcap + "(" + parameterList + ") {");
            pw.println("        return " + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + ").toString();");
            pw.println("    }");
            if (resource instanceof ResourceDef.Exception) {
                ResourceDef.Exception exception = (ResourceDef.Exception) resource;
                String errorClassName = getErrorClass(resourceList, exception);
                deduceExceptionConstructors(errorClassName);
                if (hasInstCon) {
                    pw.println("    public " + errorClassName + " new" + resourceInitcap + "(" + parameterList + ") {");
                    pw.println("        return new " + errorClassName + "(" + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + "));");
                    pw.println("    }");
                } else if (hasInstThrowCon) {
                    pw.println("    public " + errorClassName + " new" + resourceInitcap + "(" + parameterList + ") {");
                    pw.println("        return new " + errorClassName + "(" + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + "), null);");
                    pw.println("    }");
                } else if (hasStringCon) {
                    pw.println("    public " + errorClassName + " new" + resourceInitcap + "(" + parameterList + ") {");
                    pw.println("        return new " + errorClassName + "(" + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + ").toString());");
                    pw.println("    }");
                } else if (hasStringThrowCon) {
                    pw.println("    public " + errorClassName + " new" + resourceInitcap + "(" + parameterList + ") {");
                    pw.println("        return new " + errorClassName + "(" + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + ").toString(), null);");
                    pw.println("    }");
                }
                if (hasInstThrowCon) {
                    pw.println("    public " + errorClassName + " new" + resourceInitcap + "(" + addLists(parameterList, "Throwable err") + ") {");
                    pw.println("        return new " + errorClassName + "(" + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + "), err);");
                    pw.println("    }");
                } else if (hasStringThrowCon) {
                    pw.println("    public " + errorClassName + " new" + resourceInitcap + "(" + addLists(parameterList, "Throwable err") + ") {");
                    pw.println("        return new " + errorClassName + "(" + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + ").toString(), err);");
                    pw.println("    }");
                }
            }
        }
        pw.println("");
        pw.println("}");
    }

    /**
     * Figures out what constructors the exception class has. We'd
     * prefer to use
     * <code>init(ResourceDefinition rd)</code> or
     * <code>init(ResourceDefinition rd, Throwable e)</code>
     * if it has them, but we can use
     * <code>init(String s)</code> and
     * <code>init(String s, Throwable e)</code>
     * as a fall-back.
     *
     * Prints a warming message if the class cannot be loaded.
     *
     * @param errorClassName Name of exception class
     */
    private void deduceExceptionConstructors(String errorClassName)
    {
        hasInstCon = false;
        hasInstThrowCon = false;
        hasStringCon = false;
        hasStringThrowCon = false;
        try {
            Class errorClass;
            try {
                errorClass = Class.forName(errorClassName);
            } catch (ClassNotFoundException e) {
                // Might be in the java.lang package, for which we
                // allow them to omit the package name.
                errorClass = Class.forName("java.lang." + errorClassName);
            }
            Constructor[] constructors = errorClass.getConstructors();
            for (int i = 0; i < constructors.length; i++) {
                Constructor constructor = constructors[i];
                Class[] types = constructor.getParameterTypes();
                if (types.length == 1 &&
                        ResourceInstance.class.isAssignableFrom(types[0])) {
                    hasInstCon = true;
                }
                if (types.length == 1 &&
                        String.class.isAssignableFrom(types[0])) {
                    hasStringCon = true;
                }
                if (types.length == 2 &&
                        ResourceInstance.class.isAssignableFrom(types[0]) &&
                        Throwable.class.isAssignableFrom(types[1])) {
                    hasInstThrowCon = true;
                }
                if (types.length == 2 &&
                        String.class.isAssignableFrom(types[0]) &&
                        Throwable.class.isAssignableFrom(types[1])) {
                    hasStringThrowCon = true;
                }
            }
        } catch (ClassNotFoundException e) {
            if (warnedClasses.add(errorClassName)) {
                System.out.println("Warning: Could not find exception " +
                "class '" + errorClassName + "' on classpath. " +
                "Exception factory methods will not be generated.");
            }
        }
    }

    private static String addLists(String x, String y) {
        if (x == null || x.equals("")) {
            if (y == null || y.equals("")) {
                return "";
            } else {
                return y;
            }
        } else if (y == null || y.equals("")) {
            return x;
        } else {
            return x + ", " + y;
        }
    }


    /**
     * Returns the type of error which is to be thrown by this resource.
     * Result is null if this is not an error.
     */
    static String getErrorClass(
            ResourceDef.ResourceBundle resourceList,
            ResourceDef.Exception exception) {
        if (exception.className != null) {
            return exception.className;
        } else if (resourceList.exceptionClassName != null) {
            return resourceList.exceptionClassName;
        } else {
            return "mondrian.resource.ChainableRuntimeException";
        }
    }


    private void generateCommentBlock(PrintWriter pw,
                                      String name,
                                      String text,
                                      String comment) {
        pw.print("\t/** ");
        if (comment != null) {
            Util.fillText(pw, comment, "\t * ", "", 70);
            pw.println();
            pw.println("\t *");
            pw.print("\t * ");
        }
        Util.fillText(pw,
                      "<code>" + name + "</code> is '" + text + "'",
                      "\t * ", "", -1);
        pw.println("\t */");
    }


    private void generateProperties(
            ResourceGen generator,
            ResourceDef.ResourceBundle resourceList,
            Locale locale) {
        String fileName = getClassNameSansPackage(locale) + ".properties";
        File file = new File(getPackageDirectory(), fileName);
        if (file.exists() &&
                file.lastModified() >= getFile().lastModified()) {
            generator.comment(file + " is up to date");
            return;
        }
        generator.comment("Generating " + file);
        final FileOutputStream out;
        try {
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            out = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            throw new BuildException("Error while writing " + file, e);
        }
        PrintWriter pw = new PrintWriter(out);
        try {
            if (locale == null) {
                generateBaseProperties(resourceList, pw);
            } else {
                generateProperties(pw, locale);
            }
        } finally {
            pw.close();
        }
    }


    /**
     * Generates a properties file containing a line for each resource.
     */
    private void generateBaseProperties(
            ResourceDef.ResourceBundle resourceList, PrintWriter pw) {
        String fullClassName = getClassName(null);
        pw.println("# This file contains the resources for");
        pw.println("# class '" + fullClassName + "'; the base locale is '" +
                resourceList.locale + "'.");
        pw.println("# It was generated by " + ResourceGen.class);
        pw.println("# from " + getFile());
        pw.println("# on " + new Date().toString() + ".");
        pw.println();
        for (int i = 0; i < resourceList.resources.length; i++) {
            ResourceDef.Resource resource = resourceList.resources[i];
            final String name = resource.name;
            if (resource.text == null) {
                throw new BuildException(
                        "Resource '" + name + "' has no message");
            }
            final String message = resource.text.cdata;
            if (message == null) {
                continue;
            }
            pw.println(name + "=" + Util.quoteForProperties(message));
        }
        pw.println("# End " + fullClassName + ".properties");
    }


    /**
     * Generates an empty properties file named after the class and the given
     * locale.
     *
     * @pre locale != null
     */
    private void generateProperties(PrintWriter pw, Locale locale) {
        String fullClassName = getClassName(locale);
        pw.println("# This file contains the resources for");
        pw.println("# class '" + fullClassName + "' and locale '" + locale + "'.");
        pw.println("# It was generated by " + ResourceGen.class);
        pw.println("# from " + getFile());
        pw.println("# on " + new Date().toString() + ".");
        pw.println();
        pw.println("# This file is intentionally blank. Add property values");
        pw.println("# to this file to override the translations in the base");
        String basePropertiesFileName = getClassNameSansPackage(locale) + ".properties";
        pw.println("# properties file, " + basePropertiesFileName);
        pw.println();
        pw.println("# End " + fullClassName + ".properties");
    }

    private String getClassName(Locale locale) {
        String s = className;
        if (locale != null) {
            s += '_' + locale.toString();
        }
        return s;
    }


    protected void generateCpp(ResourceGen generator,
                               ResourceDef.ResourceBundle resourceList) {
        String defaultExceptionClass = resourceList.cppExceptionClassName;
        String defaultExceptionLocation = resourceList.cppExceptionClassLocation;
        if (defaultExceptionClass != null &&
            defaultExceptionLocation == null) {
            throw new BuildException(
                "C++ exception class is defined without a header file location in "
                + getFile());
        }

        for(int i = 0; i < resourceList.resources.length; i++) {
            ResourceDef.Resource resource = resourceList.resources[i];

            if (resource.text == null) {
                throw new BuildException(
                    "Resource '" + resource.name + "' has no message");
            }

            if (resource instanceof ResourceDef.Exception) {
                ResourceDef.Exception exception =
                    (ResourceDef.Exception)resource;

                if (exception.cppClassName != null &&
                    (exception.cppClassLocation == null &&
                     defaultExceptionLocation == null)) {
                    throw new BuildException(
                        "C++ exception class specified for "
                        + exception.name
                        + " without specifiying a header location in "
                        + getFile());
                }

                if (defaultExceptionClass == null &&
                    exception.cppClassName == null) {
                    throw new BuildException(
                        "No exception class specified for "
                        + exception.name
                        + " in "
                        + getFile());
                }
            }
        }


        String hFilename = cppClassName + ".h";
        String cppFileName = cppClassName + ".cpp";

        File hFile = new File(getPackageDirectory(), hFilename);
        File cppFile = new File(getPackageDirectory(), cppFileName);

        boolean allUpToDate = true;

        if (!checkUpToDate(generator, hFile)) {
            allUpToDate = false;
        }

        if (!checkUpToDate(generator, cppFile)) {
            allUpToDate = false;
        }

        if (allUpToDate) return;

        generator.comment("Generating " + hFile);

        final FileOutputStream hOut;
        try {
            makeParentDirs(hFile);

            hOut = new FileOutputStream(hFile);
        }
        catch(FileNotFoundException e) {
            throw new BuildException("Error while writing " + hFile, e);
        }

        PrintWriter pw = new PrintWriter(hOut);
        try {
            generateCppHeader(resourceList, hFile, pw);
        }
        finally {
            pw.close();
        }

        generator.comment("Generating " + cppFile);

        final FileOutputStream cppOut;
        try {
            makeParentDirs(cppFile);

            cppOut = new FileOutputStream(cppFile);
        }
        catch(FileNotFoundException e) {
            throw new BuildException("Error while writing " + cppFile, e);
        }

        pw = new PrintWriter(cppOut);
        try {
            generateCpp(resourceList, pw, hFilename);
        }
        finally {
            pw.close();
        }
    }

    private void generateCppHeader(ResourceDef.ResourceBundle resourceList,
                                   File file,
                                   PrintWriter pw) {
        generateDoNotModifyHeader(pw);
        generateGeneratedByBlock(pw);

        StringBuffer ifndef = new StringBuffer();
        String fileName = file.getName();
        ifndef.append(fileName.substring(0, fileName.length() - 2));
        ifndef.append("_Included");
        if (resourceList.cppNamespace != null) {
            ifndef.insert(0, '_');
            ifndef.insert(0, resourceList.cppNamespace.substring(1));
            ifndef.insert(0, Character.toUpperCase(resourceList
                                                   .cppNamespace
                                                   .charAt(0)));
        }

        pw.println("#ifndef " + ifndef.toString());
        pw.println("#define " + ifndef.toString());
        pw.println();
        pw.println("#include <ctime>");
        pw.println("#include <string>");
        pw.println();
        pw.println("#include \"Locale.h\"");
        pw.println("#include \"ResourceDefinition.h\"");
        pw.println("#include \"ResourceBundle.h\"");
        pw.println();

        pw.println("// begin includes specified by " + getFile());
        if (resourceList.cppExceptionClassLocation != null) {
            pw.println("#include \""
                       + resourceList.cppExceptionClassLocation
                       + "\"");
        }

        for(int i = 0; i < resourceList.resources.length; i++) {
            ResourceDef.Resource resource = resourceList.resources[i];

            if (resource instanceof ResourceDef.Exception) {
                ResourceDef.Exception exception =
                    (ResourceDef.Exception)resource;

                if (exception.cppClassLocation != null) {
                    pw.println("#include \""
                               + exception.cppClassLocation
                               + "\"");
                }
            }
        }
        pw.println("// end includes specified by " + getFile());
        pw.println();
        if (resourceList.cppNamespace != null) {
            pw.println("namespace " + resourceList.cppNamespace + " {");
            pw.println();
        }

        pw.println();

        String baseClass = (cppBaseClassName != null
                            ? cppBaseClassName
                            : "ResourceBundle");

        String className = getClassNameSansPackage(null);
        String bundleCacheClassName = className + "BundleCache";

        pw.println("class " + className + ";");
        pw.println("typedef map<Locale, " + className + "*> "
                   + bundleCacheClassName + ";");
        pw.println();
        pw.println("class " + className + " : " + baseClass);
        pw.println("{");
        pw.println("    protected:");
        pw.println("    explicit " + className + "(Locale locale);");
        pw.println();
        pw.println("    public:");
        pw.println("    virtual ~" + className + "() { }");
        pw.println();
        pw.println("    static const " + className + " &instance();");
        pw.println("    static const "
                   + className
                   + " &instance(const Locale &locale);");
        pw.println();

        pw.println("    static void setResourceFileLocation(const std::string &location);");
        pw.println();

        for(int i = 0; i < resourceList.resources.length; i++) {
            ResourceDef.Resource resource = resourceList.resources[i];

            String text = resource.text.cdata;
            String comment = ResourceGen.getComment(resource);
            String parameterList = ResourceGen.getParameterList(text, false);

            // e.g. "Internal"
            final String resourceInitCap =
                ResourceGen.getResourceInitcap(resource);

            generateCommentBlock(pw, resource.name, text, comment);

            pw.println("    std::string " + resource.name + "("
                       + parameterList + ") const;");

            if (resource instanceof ResourceDef.Exception) {
                ResourceDef.Exception exception =
                    (ResourceDef.Exception)resource;

                String exceptionClass = exception.cppClassName;
                if (exceptionClass == null) {
                    exceptionClass = resourceList.cppExceptionClassName;
                }

                pw.println("    " + exceptionClass
                           + "* new" + resourceInitCap + "("
                           + parameterList + ") const;");

                boolean chainExceptions =
                    (exception.cppChainExceptions != null &&
                     exception.cppChainExceptions.equalsIgnoreCase("true"));

                if (chainExceptions) {
                    if (parameterList.length() > 0) {
                        pw.println("    "
                                   + exceptionClass
                                   + "* new"
                                   + resourceInitCap
                                   + "("
                                   + parameterList
                                   + ", const "
                                   + exceptionClass
                                   + " * const prev) const;");
                    } else {
                        pw.println("  "
                                   + exceptionClass
                                   + " new"
                                   + resourceInitCap + "("
                                   + "const "
                                   + exceptionClass
                                   + " * const prev) const;");
                    }
                }
            }

            pw.println();
        }

        pw.println("    private:");
        for(int i = 0; i < resourceList.resources.length; i++) {
            ResourceDef.Resource resource = resourceList.resources[i];

            pw.println("    ResourceDefinition _" + resource.name + ";");
        }
        pw.println();

        pw.println("    template<class _GRB, class _BC, class _BC_ITER>");
        pw.println("        friend _GRB *makeInstance(_BC &bundleCache, const Locale &locale);");

        pw.println("};");


        if (resourceList.cppNamespace != null) {
            pw.println();
            pw.println("} // end namespace " + resourceList.cppNamespace);
        }

        pw.println();
        pw.println("#endif // " + ifndef.toString());
    }


    private void generateCpp(ResourceDef.ResourceBundle resourceList,
                             PrintWriter pw,
                             String headerFilename) {
        generateDoNotModifyHeader(pw);
        generateGeneratedByBlock(pw);

        String className = getClassNameSansPackage(null);
        String bundleCacheClassName = className + "BundleCache";
        String baseClass = (cppBaseClassName != null
                            ? cppBaseClassName
                            : "ResourceBundle");

        if (resourceList.cppCommonInclude != null) {
            pw.println("// begin common include specified by " + getFile());
            pw.println("#include \"" + resourceList.cppCommonInclude + "\"");
            pw.println("// end common include specified by " + getFile());
        }

        pw.println("#include \"" + headerFilename + "\"");
        pw.println("#include \"ResourceBundle.h\"");
        pw.println("#include \"Locale.h\"");
        pw.println();
        pw.println("#include <map>");
        pw.println("#include <string>");
        pw.println();

        if (resourceList.cppNamespace != null) {
            pw.println("namespace " + resourceList.cppNamespace + " {");
            pw.println();
        }

        pw.println("using namespace std;");
        pw.println();
        pw.println("#define BASENAME (\"" + className + "\")");
        pw.println();
        pw.println("static " + bundleCacheClassName + " bundleCache;");
        pw.println("static string bundleLocation(\"\");");
        pw.println();

        pw.println("const " + className + " &" + className + "::instance()");
        pw.println("{");
        pw.println("    return " + className + "::instance(Locale::getDefault());");
        pw.println("}");
        pw.println();
        pw.println("const " + className
                   + " &" + className + "::instance(const Locale &locale)");
        pw.println("{");
        pw.println("    return *makeInstance<"
                   + className + ", "
                   + bundleCacheClassName + ", "
                   + bundleCacheClassName
                   + "::iterator>(bundleCache, locale);");
        pw.println("}");
        pw.println();
        pw.println("void "
                   + className
                   + "::setResourceFileLocation(const string &location)");
        pw.println("{");
        pw.println("    bundleLocation = location;");
        pw.println("}");
        pw.println();

        pw.println("" + className + "::" + className + "(Locale locale)");
        pw.println("    : " + baseClass
                   + "(BASENAME, locale, bundleLocation),");

        for(int i = 0; i < resourceList.resources.length; i++) {
            ResourceDef.Resource resource = resourceList.resources[i];

            pw.print("      _"
                     + resource.name
                     + "(this, \""
                     + resource.name
                     + "\")");

            if (i + 1 < resourceList.resources.length) {
                pw.println(',');
            } else {
                pw.println();
            }
        }
        pw.println("{ }");
        pw.println();

        for (int i = 0; i < resourceList.resources.length; i++) {
            ResourceDef.Resource resource = resourceList.resources[i];
            generateCppResource(resource, pw, className,
                resourceList.cppExceptionClassName);
        }

        if (resourceList.cppNamespace != null) {
            pw.println();
            pw.println("} // end namespace " + resourceList.cppNamespace);
        }
    }

    private void generateCppResource(ResourceDef.Resource resource,
        PrintWriter pw, String className, String defaultExceptionClassName)
    {
        String text = resource.text.cdata;
        //String comment = ResourceGen.getComment(resource);

        // e.g. "Internal"
        final String resourceInitCap =
            ResourceGen.getResourceInitcap(resource);

        String parameterList = ResourceGen.getParameterList(text, false);
        String argumentList = ResourceGen.getArgumentList(text, false);

        pw.println("string " + className + "::" + resource.name + "("
                   + parameterList + ") const");
        pw.println("{");
        pw.println("    return _"
                   + resource.name
                   + ".format("
                   + argumentList
                   + ");");
        pw.println("}");

        if (resource instanceof ResourceDef.Exception) {
            ResourceDef.Exception exception =
                (ResourceDef.Exception)resource;

            String exceptionClass = exception.cppClassName;
            if (exceptionClass == null) {
                exceptionClass = defaultExceptionClassName;
            }

            pw.println(exceptionClass + "* "
                       + className + "::new" + resourceInitCap + "("
                       + parameterList + ") const");
            pw.println("{");
            pw.println("    return new "
                       + exceptionClass
                       + "("
                       + resource.name
                       + "("
                       + argumentList
                       + "));");
            pw.println("}");
            pw.println();

            boolean chainExceptions =
                (exception.cppChainExceptions != null &&
                 exception.cppChainExceptions.equalsIgnoreCase("true"));

            if (chainExceptions) {
                if (parameterList.length() > 0) {
                    pw.println(exceptionClass
                               + "* "
                               + className
                               + "::new"
                               + resourceInitCap
                               + "("
                               + parameterList
                               + ", const "
                               + exceptionClass
                               + " * const prev) const");
                } else {
                    pw.println(exceptionClass
                               + " "
                               + className
                               + "::new"
                               + resourceInitCap
                               + "(const "
                               + exceptionClass
                               + " * const prev) const");
                }
                pw.println("{");

                pw.println("    return new "
                           + exceptionClass
                           + "("
                           + resource.name
                           + "("
                           + argumentList
                           + "), prev);");
                pw.println("}");
                pw.println();
            }
        }
    }
}
