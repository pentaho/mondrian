<?xml version="1.0"?>
<!--
$Id$

This software is subject to the terms of the Common Public License
Agreement, available at the following URL:
http://www.opensource.org/licenses/cpl.html.
(C) Copyright 2002-2005 Kana Software, Inc. and others.
All Rights Reserved.
You must accept the terms of that agreement to use this software.

Prints the text of an MDX query.
-->

<!DOCTYPE xsl:stylesheet [
  <!ENTITY nbsp "&#160;">
  <!ENTITY amp "&#046;">
]>

<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:tt="http://www.tonbeller.com/bii/treetable"
>

<xsl:output method="html" indent="yes"/>


<xsl:template match="mdxtable">
  <xsl:value-of select="query"/>
</xsl:template>

</xsl:stylesheet>
