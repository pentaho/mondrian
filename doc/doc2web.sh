#!/bin/sh
# Converts documentation from Mondrian's source format to Pentaho's web site.
#
# The file structure looks like this:
# content/
#    en/
#       documentation/
#           install_doc.htm (copy of install.html)
#    es/
#       documentation/
#           install_doc.htm (copy of install_es.html)
# images/
#    arch_mondrian_v1_tn.png
#    (etc.)
# api/
#    index.html
#    overview.html
#    mondrian/
#       olap/
#          Connection.html (javadoc for mondrian.olap.Connection)
#    (etc.)

doHtml() {
  # LOCALE="en"
  LOCALE="$1"
  # SRCFILE="aggregate_tables.html"
  SRCFILE="$2"
  # DSTFILE="content/en/documentation/aggregate_tables.html"
  DSTFILE=content/${LOCALE}/documentation/$(echo $SRCFILE|sed -e s/.html$/_doc.htm/)
  if [ "$LOCALE" != "en" ]; then
    SRCFILE=$(echo $SRCFILE | sed -e "s/\.html/_${LOCALE}.html/")
  fi
  mkdir -p content/${LOCALE}/documentation
  echo :: copy $SRCFILE to $DSTFILE
  cat $SRCFILE |
  sed -e '
s! src="images/! src="../images/!;
s! href="\([^/]*\)\.html! href="\1.php!
         ' |
  awk '
/doc2web start/ {++x;next;}
/doc2web end/ {++x;next;}
{if (x == 1) print;}
      ' >$DSTFILE

}

doImg() {
  test -f "$1" || echo "Image '$1' not found"
}

ROOT=$(cd $(dirname $0); pwd -P)
cd $ROOT

# Remove output from previous run.
rm -rf content

# Build javadoc.
if false; then
  (
  cd $ROOT
  ant javadoc xml_schema
  )
fi

# E.g. doc/aggregate_tables.html
# becomes website/content/en/documentation/aggregate_tables_doc.htm

doHtml en aggregate_tables.html
doHtml en architecture.html
doHtml en cmdrunner.html
doHtml en components.html
doHtml en configuration.html
doHtml en developer.html
doHtml en developer_notes.html
doHtml en faq.html
doHtml en help.html
# skip: doHtml en index.html
doHtml en install.html
doHtml es install.html
doHtml en install_postgresql.html
doHtml en mdx.html
doHtml en olap.html
doHtml en optimizing_performance.html
doHtml en roadmap.html
doHtml en schema.html
doHtml en xml_schema.html

doImg images/aggregate_tables_1.png
doImg images/aggregate_tables_2.png
doImg images/aggregate_tables_3.png
doImg images/arch_mondrian_v1_tn.png
doImg images/arch_mondrian_sketch_tn.png
doImg images/zoom.png
doImg images/logo_mondrian_lrg.png

# Remove archive.
rm -f mondrianPentaho.tar.gz

# Create archive, containing html, images, and javadoc.
tar -cvz -f mondrianPentaho.tar.gz content images api

# Copy file to server, and deploy.
if true; then
  scp mondrianPentaho.tar.gz mondriantest@mondriantest.pentaho.org:httpdocs
  ssh mondriantest@mondriantest.pentaho.org <<EOF
    cd httpdocs
    tar xvfz mondrianPentaho.tar.gz

    # Fix up file permissions
    find api content images -type d | xargs chmod go+rx
    find api content images -type f | xargs chmod go+r
EOF
fi

# End doc2web.sh

