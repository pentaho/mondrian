/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// dsommerfield, 28 December, 2000
*/

package mondrian.xom;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * The MetaTester class is a utility class for testing generated models.
 * The tester reads a model file in XML, validates it against its DTD,
 * converts it to its corresponding model definition class (always a
 * subclass of ElementDef), and displays the results.
 * The MetaTester may be used to test a model against a suite of input
 * files to verify the model's correctness.
 */
public class MetaTester {

    // rootDef is the ElementDef class representing the root of the model.
    private Class rootDef;

    // rootConstructor is the constructor for the rootDef class which
    // takes Element as its only argument.
    private Constructor rootConstructor;

    /** The parser. */
    private Parser parser;

    // model is the root of the metamodel, and contains all basic model
    // information.
    private MetaDef.Model model;

    // modelDocType is the DocType expected for all test files
    private String modelDocType;

    /**
     * The type of parser to use.  Values are {@link XOMUtil#MSXML}, etc.
     **/
    private int parserType;

    /**
     * Constructs a new MetaTester using the given model file, the given
     * test file, and the directory containing all support files.
     * @param modelFile an XML file describing the model to be tested.
     * This model should have already been compiled using the MetaGenerator
     * utility.
     * @param fileDirectory the directory containing all output files
     * (Java classes, dtds, etc) from the model compilation.  The model
     * and its associated java class must be compiled.
     * @throws XOMException if the model file is corrupted or if any
     * of its compiled components cannot be loaded.
     */
    public MetaTester(String modelFile,
                      String fileDirectory,
                      int parserType)
        throws XOMException, IOException
    {
        // Set the parser
        this.parserType = parserType;

        // Load the input model file.
        FileInputStream in = null;
        try {
            in = new FileInputStream(modelFile);
        } catch (IOException ex) {
            throw new XOMException("Loading of model file " + modelFile
                                      + " failed: " + ex.getMessage());
        }

        // Parse the meta model.
        Parser parser = XOMUtil.createDefaultParser();
        try {
            DOMWrapper def = parser.parse(in);
            model = new MetaDef.Model(def);
        } catch (XOMException ex) {
            throw new XOMException(
                ex, "Failed to parse XML file: " + modelFile);
        }

        // Load the root java class of the Java version of this model.
        // Then find the Constructor which takes a single DOMWrapper.
        String modelRoot = getModelRoot(model);
        try {
            rootDef = Class.forName(model.className + "$" + modelRoot);
            Class[] params = new Class[1];
            params[0] = DOMWrapper.class;
            rootConstructor = rootDef.getConstructor(params);
        } catch (ClassNotFoundException ex) {
            throw new XOMException("Model class " + model.className
                                      + "." + modelRoot + " could not be "
                                      + "loaded: " + ex.getMessage());
        } catch (NoSuchMethodException ex) {
            throw new XOMException("Model class " + model.className
                                      + "." + modelRoot + " has no "
                                      + "constructor which takes a "
                                      + "DOMWrapper.");
        }

        // Figure out if the model uses plugins or imports by looking at all
        // element definitions.  If plugins or imports are in use, we can't use the
        // dtd for validation.
        boolean usesPlugins = false;
        for(int i=0; i<model.elements.length; i++) {
            if(model.elements[i] instanceof MetaDef.Plugin ||
               model.elements[i] instanceof MetaDef.Import) {
                usesPlugins = true;
                break;
            }
        }

        // Construct parser for test documents.  The exact parser we use
        // depends on the setting of the parser variable.
        // Use validation only if plugins are not used by the model.
        modelDocType = null;
        if (usesPlugins) {
            System.out.println("Plugins or imports are in use: ignoring DTD.");
        } else {
            modelDocType = getModelDocType(model);
            System.out.println("No plugins or imports: using DTD with DocType "
                               + modelDocType + ".");
        }

        parser = XOMUtil.makeParser(
            parserType, usesPlugins, fileDirectory,
            model.dtdName, modelDocType);
    }

    /**
     * Helper function to copy from a reader to a writer
     */
    private static void readerToWriter(Reader reader, Writer writer)
        throws IOException
    {
        int numChars;
        final int bufferSize = 16384;
        char[] buffer = new char[bufferSize];
        while((numChars = reader.read(buffer)) != -1) {
            if(numChars > 0)
                writer.write(buffer, 0, numChars);
        }
    }

    /**
     * This helper function retrieves the root element name from a model.  The
     * root element name may be defined explicitly, or it may need to be
     * located as the first element in the file itself.
     * Also, if a prefix is defined, we need to add it here.
     */
    private static String getModelRoot(MetaDef.Model model)
        throws XOMException
    {
        if(model.root != null)
            return model.root;
        for(int i=0; i<model.elements.length; i++) {
            if(model.elements[i] instanceof MetaDef.Element) {
                return ((MetaDef.Element)model.elements[i]).type;
            }
        }

        throw new XOMException("Model " + model.name + " has no "
                                  + "root element defined and has no first "
                                  + "element.");
    }

    /**
     * This helper function retrieves the root dtd element name from a model.
     * This is identical to the model root returned by getModelRoot, except
     * that the prefix (if any) is prepended.  If the root element has
     * a dtdName defined, this will be used instead of the prefixed name.
     */
    private static String getModelDocType(MetaDef.Model model)
        throws XOMException
    {
        if(model.root != null)
            return model.root;
        for(int i=0; i<model.elements.length; i++) {
            if(model.elements[i] instanceof MetaDef.Element) {
                MetaDef.Element elt = (MetaDef.Element)(model.elements[i]);
                if(model.root == null ||
                   model.root.equals(elt.type)) {
                    if(elt.dtdName != null)
                        return elt.dtdName;
                    else if(model.prefix != null)
                        return model.prefix + elt.type;
                    else
                        return elt.type;
                }
            }
        }

        if(model.root == null)
            throw new XOMException("Model " + model.name + " has no "
                                      + "root element defined and has no first "
                                      + "element.");
        else
            throw new XOMException("Model root element " + model.root
                                      + " is not defined as an Element.");
    }

    /**
     * Instantiate the Element into an ElementDef of the correct type.
     */
    private ElementDef instantiate(DOMWrapper elt)
        throws XOMException
    {
        ElementDef def = null;
        try {
            Object[] args = new Object[1];
            args[0] = elt;
            def = (ElementDef)(rootConstructor.newInstance(args));
        } catch (InstantiationException ex) {
            throw new XOMException("Unable to instantiate holder class "
                                      + rootDef.getName() + ": "
                                      + ex.getMessage());
        } catch (IllegalAccessException ex) {
            throw new XOMException("Unable to instantiate holder class "
                                      + rootDef.getName() + ": "
                                      + ex.getMessage());
        } catch (InvocationTargetException ex) {
            Throwable sub = ex.getTargetException();
            if(sub instanceof RuntimeException)
                throw (RuntimeException)sub;
            else if(sub instanceof XOMException)
                throw (XOMException)sub;
            else
                throw new XOMException("Exeception occurred while "
                                          + "instantiating holder class "
                                          + rootDef.getName() + ": "
                                          + sub.toString());
        }
        return def;
    }

    /**
     * Tests a specific instance of the given model, as described by
     * testFile.  Testing includes parsing testFile, validating against
     * its associated dtd, and converting to its assocated java class.
     * The contents of the java class are displayed to complete the test.
     * @param testFile the XML file to be tested.
     * @param fileDirectory directory containing files.
     * @throws XOMException if the test fails for any reason.
     */
    public void testFile(String testFile, String fileDirectory)
        throws XOMException
    {
        // Set a FILE url for the DTD, if one was provided
        File dtdPath = new File(fileDirectory, model.dtdName);
        String dtdUrl = "file:" + dtdPath.getAbsolutePath();

        // Read the file into a String.  Do so to avoid the complexity of
        // parsing directly from an input stream (rather than a reader).
        // Add an XML declaration and DocType here, unless we're using
        // MSXML.
        String xmlString = null;
        try {
            StringWriter sWriter = new StringWriter();
            FileReader reader = new FileReader(testFile);

            if(parserType != XOMUtil.MSXML) {
                PrintWriter out = new PrintWriter(sWriter);
                out.println("<?xml version=\"1.0\" ?>");
                if(modelDocType != null)
                    out.println("<!DOCTYPE " + modelDocType
                                + " SYSTEM \"" + dtdUrl + "\">");
                out.flush();
            }

            readerToWriter(reader, sWriter);
            reader.close();
            xmlString = sWriter.toString();
        } catch (IOException ex) {
            throw new XOMException("Unable to read input test "
                                      + testFile + ": " + ex.getMessage());
        }

        DOMWrapper elt = parser.parse(xmlString);

        // Instantiate the ElementDef class using its Element constructor.
        ElementDef def = instantiate(elt);

        // Display the results
        System.out.println("Testing model " + testFile);
        System.out.println("Display:");
        System.out.println(def.toString());
        System.out.println();

        // Display the results in XML as well
        String xmlOut = def.toXML();
        System.out.println();
        System.out.println("Regurgitated XML:");
        System.out.println(xmlOut);

        // Parse the generated XML back into another ElementDef.
        // To do so, we must add the xml PI and DOCTYPE at the top of the
        // String (unless we're using MSXML).
        if(parserType != XOMUtil.MSXML) {
            StringWriter writer = new StringWriter();
            PrintWriter out = new PrintWriter(writer);
            out.println("<?xml version=\"1.0\" ?>");
            if(modelDocType != null)
                out.println("<!DOCTYPE " + modelDocType
                            + " SYSTEM \"" + dtdUrl + "\">");
            out.println(xmlOut);
            out.flush();
            xmlOut = writer.toString();
        }
        DOMWrapper elt2 = parser.parse(xmlOut);

        // Instantiate the second ElementDef class
        ElementDef def2 = instantiate(elt2);

        // Verify equality, and then test equality
        try {
            def.verifyEqual(def2);
        }
        catch(XOMException ex) {
            System.err.println("Equality failure.  Regurgitated XML:");
            System.err.println(xmlOut);
            throw ex;
        }
        if(!def.equals(def2))
            throw new XOMException("Equality check failed even though "
                                      + "verifyEqual passed.");

        // Test serialization on def (silently). Use reflection to get around
        // build dependencies.
        try {
        Class classTSerialization = Class.forName(
            "Broadbase.mining.test.TSerialization");
        java.lang.reflect.Method method = classTSerialization.getMethod(
            "testSerialization", new Class[] {
                java.lang.Object.class, java.io.PrintWriter.class});
        method.invoke(null, new Object[] {def, null});
        } catch (ClassNotFoundException ex) {
            throw new XOMException(ex, "serialization failed");
        } catch (NoSuchMethodException ex) {
            throw new XOMException(ex, "serialization failed");
        } catch (IllegalAccessException ex) {
            throw new XOMException(ex, "serialization failed");
        } catch (InvocationTargetException ex) {
            throw new XOMException(ex, "serialization failed");
        }
    }

    /**
     * The MetaTester tests a suite of test model files against a
     * compiled model.
     * <p>Arguments:
     * <ol>
     * <li>The name of the model description file.  This is an XML file
     *     describing the model itself.
     * <li>The name of the output directory.  This output directory should
     *     contain all files generated when compiling the model.
     * </ol>
     * <p>All other arguments are the names of the test model files.  Each
     * of these will be tested and displayed in turn.
     */
    public static void main(String[] args)
        throws XOMException, IOException
    {
        int firstArg = 0;
        if(args.length > 0 && args[0].equals("-debug")) {
            System.err.println("MetaTester pausing for debugging.  "
                               + "Attach your debugger "
                               + "and press return.");
            try {
                System.in.read();
                firstArg++;
            }
            catch(IOException ex) {
                // Do nothing
            }
        }

        int parser = XOMUtil.MSXML;
        if (firstArg < args.length && args[firstArg].equals("-msxml")) {
            parser = XOMUtil.MSXML;
            firstArg++;
        }
        else if (firstArg < args.length && args[firstArg].equals("-xerces")) {
            parser = XOMUtil.XERCES;
            firstArg++;
        }

        if(args.length < firstArg+2) {
            System.err.println(
                "Usage: java MetaTester [-debug] [-msxml | -xerces] "
                + "<model XML file> <output dir> <tests> ...");
            System.exit(-1);
        }

        MetaTester tester = new MetaTester(args[0+firstArg], args[1+firstArg],
                                           parser);
        for(int i=2+firstArg; i<args.length; i++)
            tester.testFile(args[i], args[1+firstArg]);
    }


}


// End MetaTester.java
