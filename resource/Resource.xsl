<?xml version='1.0'?>
<!-- $Id$ -->
<!--  This software is subject to the terms of the Common Public License -->
<!--  Agreement, available at the following URL: -->
<!--  http://www.opensource.org/licenses/cpl.html. -->
<!-- (C) Copyright 2000-2005 Kana Software, Inc. and others. -->
<!--  All Rights Reserved. -->
<!--  You must accept the terms of that agreement to use this software. -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/TR/WD-xsl">
<xsl:template match="/">

<TABLE BORDER="1">
<TR><TH>Static?</TH><TD><xsl:value-of select="resourceBundle/@static"/></TD></TR>
<TR><TH>Locale</TH><TD><xsl:value-of select="resourceBundle/locale"/></TD></TR>
<TR><TH>Exception class</TH><TD><xsl:value-of select="resourceBundle/@exceptionClassName"/></TD></TR>
</TABLE>

<P/>

<TABLE BORDER="1">
<TR>
<TH>Id</TH>
<TH ALIGN="LEFT">Name</TH>
<TH ALIGN="LEFT">Message text</TH>
</TR>
<xsl:for-each select="resourceBundle/exception">
<TR>
<TD><xsl:value-of select="@id"/></TD>
<TD><xsl:value-of select="@name"/></TD>
<TD><PRE><xsl:for-each select="text"><xsl:value-of select="text()"/></xsl:for-each></PRE></TD>
</TR>
</xsl:for-each>
</TABLE>
</xsl:template>
</xsl:stylesheet>
<!-- End Resource.xsl -->
