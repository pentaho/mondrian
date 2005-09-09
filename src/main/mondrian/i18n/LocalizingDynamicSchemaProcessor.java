/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.i18n;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.DynamicSchemaProcessor;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Schema processor which helps localize data and metadata.
 *
 * @author arosselet
 * @since August 26, 2005
 * @version $Id$
 */
public class LocalizingDynamicSchemaProcessor
        implements DynamicSchemaProcessor {
    private static final Logger LOGGER =
            Logger.getLogger(LocalizingDynamicSchemaProcessor.class);

    /** Creates a new instance of LocalizingDynamicSchemaProcessor */
    public LocalizingDynamicSchemaProcessor() {
    }

    private PropertyResourceBundle i8n;

    /**
     * Regular expression for variables.
     */
    private static final Pattern pattern = Pattern.compile("(%\\{.*?\\})");
    private static final int INVALID_LOCALE = 1;
    private static final int FULL_LOCALE = 3;
    private static final int LANG_LOCALE = 2;
    private static final Set countries = Collections.unmodifiableSet(
            new HashSet(Arrays.asList(Locale.getISOCountries())));
    private static final Set languages = Collections.unmodifiableSet(
            new HashSet(Arrays.asList(Locale.getISOLanguages())));
    private int localeType = INVALID_LOCALE;

    void populate(String propFile) {
        String localizedPropFileBase = "";
        String [] tokens = propFile.split("\\.");

        for (int i = 0; i < tokens.length - 1; i++) {
            localizedPropFileBase = localizedPropFileBase +
                    ((localizedPropFileBase.length() == 0) ? "" : ".") +
                    tokens[i];
        }

        String [] localePropFilename = new String[localeType];
        String [] localeTokens = locale.split("\\_");
        int index = localeType;
        for (int i = 0; i <localeType;i++) {
            //"en_GB" -> [en][GB]  first
            String catName = "";
            /*
             * if en_GB, then append [0]=_en_GB [1]=_en
             * if en, then append [0]=_en
             * if null/bad then append nothing;
             */
            for (int j = 0;j <= i - 1; j++) {
                catName += "_" + localeTokens[j];
            }
            localePropFilename[--index] = localizedPropFileBase + catName +
                    "." + tokens[tokens.length-1];
        }
        boolean fileExists = false;
        File file = null;
        for (int i = 0;i < localeType && !fileExists; i++) {
            file = new File(localePropFilename[i]);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("populate: file=" +
                        file.getAbsolutePath() +
                        " exists=" +
                        file.exists()
                        );
            }
            if (!file.exists()) {
                LOGGER.warn("Mondrian: Warning: file '"
                        + file.getAbsolutePath()
                        + "' not found - trying next default locale");
            }
            fileExists = file.exists();
        }

        if (fileExists) {
            try {
                URL url = Util.toURL(file);
                i8n = new PropertyResourceBundle(url.openStream());
                LOGGER.info("Mondrian: locale file '"
                        + file.getAbsolutePath()
                        + "' loaded");

            } catch (MalformedURLException e) {
                LOGGER.error("Mondrian: locale file '"
                        + file.getAbsolutePath()
                        + "' could not be loaded ("
                        + e
                        + ")");
            } catch (java.io.IOException e){
                LOGGER.error("Mondrian: locale file '"
                        + file.getAbsolutePath()
                        + "' could not be loaded ("
                        + e
                        + ")");
            }
        } else {
            LOGGER.warn("Mondrian: Warning: no suitable locale file found for locale '"
                    + locale
                    + "'");
        }
    }

    private void loadProperties() {
        String propFile = MondrianProperties.instance().LocalePropFile.get();
        if (propFile != null) {
            populate(propFile);
        }
    }

    public String processSchema(
            URL schemaUrl, Util.PropertyList connectInfo) throws Exception {

        setLocale(connectInfo.get("Locale"));

        loadProperties();

        StringBuffer buf = new StringBuffer();
        BufferedReader in = new BufferedReader(
                new InputStreamReader(
                        schemaUrl.openStream()));
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            buf.append(inputLine);
        }
        in.close();
        String schema = buf.toString();
        if (i8n != null) {
            schema = doRegExReplacements(schema);
        }
        LOGGER.debug(schema);
        return schema;
    }

    private String doRegExReplacements(String schema) {
        StringBuffer intlSchema = new StringBuffer();
        Matcher match = pattern.matcher(schema);
        String key;
        while (match.find()) {
            key = extractKey(match.group());
            int start = match.start();
            int end = match.end();

            try {
                String intlProperty = i8n.getString(key);
                if (intlProperty!=null){
                    match.appendReplacement(intlSchema, intlProperty);
                }
            } catch (java.util.MissingResourceException e){
                LOGGER.error("Missing resource for key ["+key+"]",e);
            } catch (java.lang.NullPointerException e){
                LOGGER.error("missing resource key at substring("+start+","+end+")",e);
            }
        }
        match.appendTail(intlSchema);
        return intlSchema.toString();
    }

    private String extractKey(String group) {
        // removes leading '%{' and tailing '%' from the matched string
        // to obtain the required key
        String key = group.substring(2, group.length() - 1);
        return key;
    }

    /**
     * Property locale.
     */
    private String locale;

    /**
     * Returns the property locale.
     *
     * @return Value of property locale.
     */
    public String getLocale() {
        return this.locale;
    }

    /**
     * Sets the property locale.
     *
     * @param locale New value of property locale.
     */
    public void setLocale(String locale) {
        this.locale = locale;
        localeType = INVALID_LOCALE;  // if invalid/missing, default localefile will be tried.
        // make sure that both language and country fields are valid
        if (locale.indexOf("_") != -1 && locale.length() == 5) {
            if (languages.contains(locale.substring(0, 2)) &&
                    countries.contains(locale.substring(3, 5))) {
                localeType = FULL_LOCALE;
            }
        } else {
            if (locale!=null && locale.length()==2){
            //make sure that the language field is valid since that is all that was provided
                if (languages.contains(locale.substring(0, 2))) {
                    localeType = LANG_LOCALE;
                }
            }
        }
    }
}

// End LocalizingDynamicSchemaProcessor.java
