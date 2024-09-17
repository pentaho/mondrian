/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package mondrian.spi;

import mondrian.olap.Member;

/**
 * An SPI to redefine the caption displayed for members.
 *
 * <p>For example, the following class displays members of the time
 * dimension as "01-JAN-2005".
 *
 * <blockquote>
 * <code>
 * public class TimeMemberFormatter implements MemberFormatter {<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;public String formatMember(Member member) {<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;SimpleDateFormat
 * inFormat =<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;new
 * SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S");<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;SimpleDateFormat
 * outFormat =<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;new
 * SimpleDateFormat("dd-MMM-yyyy");<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;try {<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Date
 * date = inFormat.parse(in.getName());<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * return outFormat.format(data);<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;} catch
 * (ParseException e) {<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * e.printStackTrace();<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * return "error";<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;}<br/>
 * }<br/>
 * </code>
 * </blockquote>
 *
 * @author hhaas
 * @since 6 October, 2004
 */
public interface MemberFormatter {
    /**
     * Returns the string to be displayed as a caption for a given member.
     */
    String formatMember(Member member);
}

// End MemberFormatter.java

