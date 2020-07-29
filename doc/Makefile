# Makefile for Antora documentation
#

# You can set these variables from the command line.
ANTORAOPTS    = DOCSEARCH_ENABLED=true DOCSEARCH_ENGINE=lunr DOCSEARCH_INDEX_VERSION=latest
ANTORAYAML    = site.yml
ANTORACMD     = antora

# Internal variables.
BUILDDIR	= build

YAML_DOC_INPUT=../conf/cassandra.yaml
YAML_DOC_OUTPUT=source/modules/cassandra/pages/configuration/cass_yaml_file.adoc

MAKE_CASSANDRA_YAML = python convert_yaml_to_adoc.py $(YAML_DOC_INPUT) $(YAML_DOC_OUTPUT)

GENERATE_NODETOOL_DOCS = python gen-nodetool-docs.py

WEB_SITE_PRESENCE_FILE='source/.build_for_website'

.PHONY: help
help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  html       to make standalone HTML files"
	@echo "  website    to make HTML files for the Cassandra website"
	@echo "  dirhtml    to make HTML files named index.html in directories"
	@echo "  singlehtml to make a single large HTML file"
	@echo "  pickle     to make pickle files"
	@echo "  json       to make JSON files"
	@echo "  htmlhelp   to make HTML files and a HTML help project"
	@echo "  qthelp     to make HTML files and a qthelp project"
	@echo "  applehelp  to make an Apple Help Book"
	@echo "  devhelp    to make HTML files and a Devhelp project"
	@echo "  epub       to make an epub"
	@echo "  epub3      to make an epub3"
	@echo "  latex      to make LaTeX files, you can set PAPER=a4 or PAPER=letter"
	@echo "  latexpdf   to make LaTeX files and run them through pdflatex"
	@echo "  latexpdfja to make LaTeX files and run them through platex/dvipdfmx"
	@echo "  text       to make text files"
	@echo "  man        to make manual pages"
	@echo "  texinfo    to make Texinfo files"
	@echo "  info       to make Texinfo files and run them through makeinfo"
	@echo "  gettext    to make PO message catalogs"
	@echo "  changes    to make an overview of all changed/added/deprecated items"
	@echo "  xml        to make Docutils-native XML files"
	@echo "  pseudoxml  to make pseudoxml-XML files for display purposes"
	@echo "  linkcheck  to check all external links for integrity"
	@echo "  doctest    to run all doctests embedded in the documentation (if enabled)"
	@echo "  coverage   to run coverage check of the documentation (if enabled)"
	@echo "  dummy      to check syntax errors of document sources"

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)/*
	rm -f $(YAML_DOC_OUTPUT)

.PHONY: gennodetool
gennodetool:
	$(GENERATE_NODETOOL_DOCS)

.PHONY: html
html:
	$(MAKE_CASSANDRA_YAML)
	#$(GENERATE_NODETOOL_DOCS)
	$(ANTORAOPTS) $(ANTORACMD) $(ANTORAYAML)
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

.PHONY: website
website: clean
	@touch $(WEB_SITE_PRESENCE_FILE)
	$(MAKE_CASSANDRA_YAML)
	$(GENERATE_NODETOOL_DOCS)
	$(ANTORAOPTS) $(ANTORACMD) $(ANTORAYAML)
	@rm $(WEB_SITE_PRESENCE_FILE)
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."
