<?xml version="1.0"?>
<!--
$Id$

This software is subject to the terms of the Common Public License
Agreement, available at the following URL:
http://www.opensource.org/licenses/cpl.html.
(C) Copyright 2002 Kana Software, Inc. and others.
All Rights Reserved.
You must accept the terms of that agreement to use this software.
-->

<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:tt="http://www.tonbeller.com/bii/treetable"
>

<xsl:output method="html" indent="yes"/>

<xsl:template match="/mdxtable">
  <xsl:apply-templates select="slicers/position/member"/>
</xsl:template>

<xsl:template match="member">
  <xsl:if test="preceding-sibling::member">
    <xsl:text>, </xsl:text>
  </xsl:if>
  <xsl:value-of select="@caption"/>
</xsl:template>


</xsl:stylesheet>
