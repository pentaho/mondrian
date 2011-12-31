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

import mondrian.olap.Util;

import java.util.*;

/**
 * Mapping between {@link Locale} and Locale identifier (LCID).
 *
 * @deprecated Soon to be moved to Olap4jUtil.
 *
 * @version $Id$
 * @author jhyde
 */
public class LcidLocale {
    final Map<Short, String> lcidLocaleMap = new HashMap<Short, String>();

    private static final Object[] LOCALE_DATA = {
        "ar", (short) 0x0401,
        "bg", (short) 0x0402,
        "ca", (short) 0x0403,
        "zh", (short) 0x0404,
        "cs", (short) 0x0405,
        "da", (short) 0x0406,
        "de", (short) 0x0407,
        "el", (short) 0x0408,
        "es", (short) 0x040a,
        "fi", (short) 0x040b,
        "fr", (short) 0x040c,
        "iw", (short) 0x040d,
        "hu", (short) 0x040e,
        "is", (short) 0x040f,
        "it", (short) 0x0410,
        "ja", (short) 0x0411,
        "ko", (short) 0x0412,
        "nl", (short) 0x0413,
        "no", (short) 0x0414,
        "pl", (short) 0x0415,
        "pt", (short) 0x0416,
        "rm", (short) 0x0417,
        "ro", (short) 0x0418,
        "ru", (short) 0x0419,
        "hr", (short) 0x041a,
        "sk", (short) 0x041b,
        "sq", (short) 0x041c,
        "sv", (short) 0x041d,
        "th", (short) 0x041e,
        "tr", (short) 0x041f,
        "ur", (short) 0x0420,
        "in", (short) 0x0421,
        "uk", (short) 0x0422,
        "be", (short) 0x0423,
        "sl", (short) 0x0424,
        "et", (short) 0x0425,
        "lv", (short) 0x0426,
        "lt", (short) 0x0427,
        "fa", (short) 0x0429,
        "vi", (short) 0x042a,
        "hy", (short) 0x042b,
        "eu", (short) 0x042d,
        "mk", (short) 0x042f,
        "tn", (short) 0x0432,
        "xh", (short) 0x0434,
        "zu", (short) 0x0435,
        "af", (short) 0x0436,
        "ka", (short) 0x0437,
        "fo", (short) 0x0438,
        "hi", (short) 0x0439,
        "mt", (short) 0x043a,
        "se", (short) 0x043b,
        "gd", (short) 0x043c,
        "ms", (short) 0x043e,
        "kk", (short) 0x043f,
        "ky", (short) 0x0440,
        "sw", (short) 0x0441,
        "tt", (short) 0x0444,
        "bn", (short) 0x0445,
        "pa", (short) 0x0446,
        "gu", (short) 0x0447,
        "ta", (short) 0x0449,
        "te", (short) 0x044a,
        "kn", (short) 0x044b,
        "ml", (short) 0x044c,
        "mr", (short) 0x044e,
        "sa", (short) 0x044f,
        "mn", (short) 0x0450,
        "cy", (short) 0x0452,
        "gl", (short) 0x0456,
        "dv", (short) 0x0465,
        "qu", (short) 0x046b,
        "mi", (short) 0x0481,
        "ar_IQ", (short) 0x0801,
        "zh_CN", (short) 0x0804,
        "de_CH", (short) 0x0807,
        "en_GB", (short) 0x0809,
        "es_MX", (short) 0x080a,
        "fr_BE", (short) 0x080c,
        "it_CH", (short) 0x0810,
        "nl_BE", (short) 0x0813,
        "no_NO_NY", (short) 0x0814,
        "pt_PT", (short) 0x0816,
        "ro_MD", (short) 0x0818,
        "ru_MD", (short) 0x0819,
        "sr_CS", (short) 0x081a,
        "sv_FI", (short) 0x081d,
        "az_AZ", (short) 0x082c,
        "se_SE", (short) 0x083b,
        "ga_IE", (short) 0x083c,
        "ms_BN", (short) 0x083e,
        "uz_UZ", (short) 0x0843,
        "qu_EC", (short) 0x086b,
        "ar_EG", (short) 0x0c01,
        "zh_HK", (short) 0x0c04,
        "de_AT", (short) 0x0c07,
        "en_AU", (short) 0x0c09,
        "fr_CA", (short) 0x0c0c,
        "sr_CS", (short) 0x0c1a,
        "se_FI", (short) 0x0c3b,
        "qu_PE", (short) 0x0c6b,
        "ar_LY", (short) 0x1001,
        "zh_SG", (short) 0x1004,
        "de_LU", (short) 0x1007,
        "en_CA", (short) 0x1009,
        "es_GT", (short) 0x100a,
        "fr_CH", (short) 0x100c,
        "hr_BA", (short) 0x101a,
        "ar_DZ", (short) 0x1401,
        "zh_MO", (short) 0x1404,
        "de_LI", (short) 0x1407,
        "en_NZ", (short) 0x1409,
        "es_CR", (short) 0x140a,
        "fr_LU", (short) 0x140c,
        "bs_BA", (short) 0x141a,
        "ar_MA", (short) 0x1801,
        "en_IE", (short) 0x1809,
        "es_PA", (short) 0x180a,
        "fr_MC", (short) 0x180c,
        "sr_BA", (short) 0x181a,
        "ar_TN", (short) 0x1c01,
        "en_ZA", (short) 0x1c09,
        "es_DO", (short) 0x1c0a,
        "sr_BA", (short) 0x1c1a,
        "ar_OM", (short) 0x2001,
        "en_JM", (short) 0x2009,
        "es_VE", (short) 0x200a,
        "ar_YE", (short) 0x2401,
        "es_CO", (short) 0x240a,
        "ar_SY", (short) 0x2801,
        "en_BZ", (short) 0x2809,
        "es_PE", (short) 0x280a,
        "ar_JO", (short) 0x2c01,
        "en_TT", (short) 0x2c09,
        "es_AR", (short) 0x2c0a,
        "ar_LB", (short) 0x3001,
        "en_ZW", (short) 0x3009,
        "es_EC", (short) 0x300a,
        "ar_KW", (short) 0x3401,
        "en_PH", (short) 0x3409,
        "es_CL", (short) 0x340a,
        "ar_AE", (short) 0x3801,
        "es_UY", (short) 0x380a,
        "ar_BH", (short) 0x3c01,
        "es_PY", (short) 0x3c0a,
        "ar_QA", (short) 0x4001,
        "es_BO", (short) 0x400a,
        "es_SV", (short) 0x440a,
        "es_HN", (short) 0x480a,
        "es_NI", (short) 0x4c0a,
        "es_PR", (short) 0x500a,
    };

    /**
     * The singleton instance. Initialized lazily, to avoid the space overhead
     * of the full map. (Most people only use LCID 1033 = en_US.)
     */
    private static LcidLocale INSTANCE;

    private LcidLocale() {
        for (int i = 0; i < LOCALE_DATA.length;) {
            String s = (String) LOCALE_DATA[i++];
            Short lcid = (Short) LOCALE_DATA[i++];
            lcidLocaleMap.put(lcid, s);
        }
    }

    /**
     * Returns the singleton instance, creating if necessary.
     */
    public static LcidLocale instance() {
        if (INSTANCE == null) {
            INSTANCE = new LcidLocale();
        }
        return INSTANCE;
    }

    /**
     * Converts a locale id to a locale.
     *
     * @param lcid LCID
     * @return Locale, never null
     * @throws RuntimeException if LCID is not valid
     */
    public Locale toLocale(short lcid) {
        final String s = lcidLocaleMap.get(lcid);
        if (s == null) {
            throw new RuntimeException("Unknown LCID " + lcid);
        }
        return Util.parseLocale(s);
    }
}

// End LcidLocale.java
