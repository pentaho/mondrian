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
package mondrian.rolap.format;

import mondrian.olap.Member;
import mondrian.rolap.RolapMemberBase;
import mondrian.spi.MemberFormatter;

/**
 * Default implementation of SPI {@link MemberFormatter}.
 * Used to make a minimum formatting in case no custom formatter is specified.
 *
 * Must be used for {@link RolapMemberBase} only.
 */
class DefaultRolapMemberFormatter implements MemberFormatter {
    private DefaultFormatter numberFormatter;

    DefaultRolapMemberFormatter(DefaultFormatter numberFormatter) {
        this.numberFormatter = numberFormatter;
    }

    /**
     * Takes rolap member's caption object to format using number formatter
     * (eliminating scientific notations and unwanted decimal values)
     * and returns formatted string value.
     *
     * <p>
     *   We rely on that this formatter will only be used
     *   for {@link RolapMemberBase} objects formatting.
     *
     *   Because we can't simply fallback to a raw caption,
     *   if we are though in a context of {@link Member#getCaption()},
     *   because it would end up with a stack overflow.
     *
     *   So, now this fromatter set by default
     *   in {@link mondrian.rolap.RolapLevel} only,
     *   and IS only used for RolapMemberBase.
     * </p>
     */
    @Override
    public String formatMember(Member member) {
        if (member instanceof RolapMemberBase) {
            RolapMemberBase rolapMember = (RolapMemberBase) member;
            return doFormatMember(rolapMember);
        }
        throw new IllegalArgumentException(
            "Rolap formatter must only be used "
            + "for RolapMemberBase formatting");
    }

    private String doFormatMember(RolapMemberBase rolapMember) {
        Object captionValue = rolapMember.getCaptionValue();
        return numberFormatter.format(captionValue);
    }
}
// End DefaultRolapMemberFormatter.java
