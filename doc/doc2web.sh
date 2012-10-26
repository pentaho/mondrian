#!/bin/bash
#
# This software is subject to the terms of the Eclipse Public License v1.0
# Agreement, available at the following URL:
# http://www.eclipse.org/legal/epl-v10.html.
# You must accept the terms of that agreement to use this software.
#
# Copyright (C) 2005-2012 Pentaho
# All Rights Reserved.
#
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
#    head/
#       en/
#           documentation/
#               install_doc.htm
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
# head/
#     api/
#         mondrian/
#             olap/
#                 Connection.html

help() {
cat <<EOF
Usage: doc2web.sh [option]... site

Options:
  --generate Whether to generate tar file
  --upload   Upload tar file
  --deploy   Unpack tar file into website
  --content  If generating, include non-javadoc content
  --javadoc  If generating, include javadoc content
  --head     Whether to deploy to 'head' section of web site
  --help     Print this help
EOF
}

pause() {
  xmessage -geom 400x400 Continue...
}

beep() {
  echo x | tr x \\007
}

doHtml() {
  # LOCALE="en"
  LOCALE="$1"
  # SRCFILE="aggregate_tables.html"
  SRCFILE="$2"
  # DSTFILE="content/en/documentation/aggregate_tables_doc.htm"
  DSTFILE=httpdocs/content/${LOCALE}/${PREFIX}documentation/$(echo $SRCFILE|sed -e s/.html$/_doc.htm/)
  mkdir -p httpdocs/content/${LOCALE}/${PREFIX}documentation
  if [ "$LOCALE" != "en" ]; then
    SRCFILE=$(echo $SRCFILE | sed -e "s/\.html/_${LOCALE}.html/")
  fi
  mkdir -p content/${LOCALE}/documentation
  echo :: copy $SRCFILE to $DSTFILE
  case "$SRCFILE" in
  xml_schema.html)
    cp "$SRCFILE" "$DSTFILE";;
  *)
    cat "$SRCFILE" |
    awk '
/doc2web start/ {++x;next;}
/doc2web end/ {++x;next;}
{if (x == 1) print;}
        ' >"$DSTFILE";;
  esac
  n=$(awk '/doc2web include/ {print FNR; exit}' $DSTFILE)
  if [ "$n" ]; then
    mv $DSTFILE /tmp/$$
    (
        head --lines=$(expr $n - 1) /tmp/$$
        cat properties.html
        tail --lines=+$(expr $n + 1) /tmp/$$
        rm /tmp/$$
    ) > $DSTFILE
  fi
}

doImg() {
  test -f "$1" || echo "Image '$1' not found"
}

genJavadoc() {
  (
  cd $ROOT/..
  rm -rf doc/api
  mkdir -p doc/api
  #ant javadoc-with-ydoc xml_schema
  ant javadoc xml_schema

  # Replace references to documents from javadoc.
  find doc/api -name \*.html |
  xargs perl -p -i -e '
s!architecture.html!../documentation/architecture.php!;
s!cache_control.html!../documentation/cache_control.php!;
s!mdx.html!../documentation/mdx.php!;
s!xml_schema.html!../documentation/xml_schema.php!;
s!schema.html!../documentation/schema.php!;
s!configuration.html!../documentation/configuration.php!;
                        '
  )
}

genContent() {
  # E.g. doc/aggregate_tables.html
  # becomes website/content/en/documentation/aggregate_tables_doc.htm
  
  doHtml en aggregate_tables.html
  doHtml en architecture.html
  doHtml en cmdrunner.html
  doHtml en cache_control.html
  doHtml en components.html
  doHtml en configuration.html
  doHtml en developer.html
  doHtml en developer_notes.html
  doHtml en faq.html
  doHtml en help.html
  # skip: doHtml en index.html
  doHtml en install.html
  doHtml es install.html
  doHtml fr install.html
  doHtml en install_postgresql.html
  doHtml en mdx.html
  doHtml en olap.html
  doHtml en optimizing_performance.html
  doHtml en roadmap.html
  doHtml en schema.html
  doHtml en xml_schema.html
  
  for i in images/*.png; do
    doImg $i
  done

  # Change references to javadoc from documents.
  find httpdocs/content -name \*.htm |
  xargs perl -p -i -e '
s! src="images/! src="../images/!g;
s! href="images/! href="/images/!g;
s! href="api! href="../api!g;
s! href="cmdrunner\.html! href="command_runner.php!g;
s! href="developer\.html! href="developers_guide.php!g;
s! href="install\.html! href="installation.php!g;
s! href="performance\.html! href="optimizing_performance.php!g;
s! href="([^/]*)\.html! href="\1.php!g;
                        '
}

ROOT=$(cd $(dirname $0); pwd -P)
cd $ROOT

# URL of site.
site=

# Whether to generate a tar file.
generate=

# Whether to generate & deploy non-javadoc content.
content=

# Whether to generate & deploy javadoc.
javadoc=

# Whether to upload the tar file.
upload=

# Whether to deploy the tar file.
deploy=

# Whether to deploy head, i.e. http://mondrian.pentaho.com/head.
# If blank, deploy stable, i.e. http://mondrian.pentaho.com.
head=

while [ $# -gt 0 ]; do
  case "$1" in
  (--generate) shift ; generate=true ;;
  (--upload) shift ; upload=true ;;
  (--deploy) shift ; deploy=true ;;
  (--content) shift ; content=true ;;
  (--javadoc) shift ; javadoc=true ;;
  (--head) shift ; head=true ;;
  (--help) shift ; help ; exit 0 ;;
  (--*) echo "Error: Unknown option '$1'"; help ; exit 1 ;;
  (*) site="$1" ; shift ;;
  esac
done

# Check environment.
if [ -n "${FTP_SITE}" -o \
     -n "${FTP_USER}" -o \
    -n "${FTP_PASSWORD}" ]
then
  echo "Please define FTP_SITE, FTP_USER and FTP_PASSWORD in your environment."
  exit 1
fi

# Remove output from previous run.
rm -rf httpdocs

# Create, copy and deploy javadoc.
mkdir httpdocs

if [ "$generate" ]; then
  if [ "$javadoc" ]; then
    genJavadoc
    if [ "$head" ]; then
      mkdir -p httpdocs/head
      mv api httpdocs/head/api
    else
      mkdir -p httpdocs
      mv api httpdocs/api
    fi
  fi

  if [ "$content" ]; then
    if [ "$head" ]; then
      PREFIX=head/
    else
      PREFIX=
    fi
    genContent
  fi

  # Remove archive.
  rm -f mondrianPentaho.tar.gz

  # Create archive, containing html, images, and javadoc.
  tar -cvz -f mondrianPentaho.tar.gz httpdocs
fi

# Copy file to server.
if [ "$upload" ]; then
  if false; then
    rsync -arP -e ssh mondrianPentaho.tar.gz ${site}:private
  else
    # Upload via FTP site. (Cures VPN issues.)

    ftp -v -n ${FTP_SITE} <<EOF | tee /tmp/ftp1.log
user "${FTP_USER}" "${FTP_PASSWORD}"
binary
put ${ROOT}/mondrianPentaho.tar.gz mondrianPentaho.tar.gz
quit
EOF

    ssh ${site} <<EOF
cd /private
rm mondrianPentaho.tar.gz
wget http://www.${FTP_SITE}/mondrianPentaho.tar.gz
EOF

    ftp -v -n ${FTP_SITE} <<EOF | tee /tmp/ftp2.log
user "${FTP_USER}" "${FTP_PASSWORD}"
delete mondrianPentaho.tar.gz
quit
EOF
  fi
fi

# Run script to unpack and deploy.
if [ "$deploy" ]; then
  if [ "$head" ]; then
    ssh ${site} <<EOF
cd /
tar xvz --overwrite -f private/mondrianPentaho.tar.gz

# Fix up file permissions
find httpdocs/{head/api,content/en/head,images} -type d | xargs chmod go+rx
find httpdocs/{head/api,content/en/head,images} -type f | xargs chmod go+r
EOF
  else
    ssh ${site} <<EOF
cd /
tar xvz --overwrite -f private/mondrianPentaho.tar.gz

# Fix up file permissions
find httpdocs/{api,content,images} -type d | xargs chmod go+rx
find httpdocs/{api,content,images} -type f | xargs chmod go+r
EOF
  fi
fi

# End doc2web.sh
