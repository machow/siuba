#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copied from https://github.com/spatialaudio/nbsphinx/blob/0.2.7/doc/conf.py

# Select nbsphinx and, if needed, add a math extension (mathjax or pngmath):
extensions = [
    'nbsphinx',
    #'sphinx.ext.mathjax',
]

# Exclude build directory and Jupyter backup files:
exclude_patterns = ['_build', '**.ipynb_checkpoints', '**.swp']

# Default language for syntax highlighting in reST and Markdown cells
highlight_language = 'none'

# -- These set defaults that can be overridden through notebook metadata --

# See http://nbsphinx.readthedocs.org/en/latest/allow-errors.html
# and http://nbsphinx.readthedocs.org/en/latest/timeout.html for more details.

# If True, the build process is continued even if an exception occurs:
#nbsphinx_allow_errors = True

# Controls when a cell will time out (defaults to 30; use -1 for no timeout):
#nbsphinx_timeout = 60

# Default Pygments lexer for syntax highlighting in code cells
#nbsphinx_codecell_lexer = 'ipython3'

# -- The settings below this line are not specific to nbsphinx ------------

master_doc = 'index'

project = 'siuba'
author = 'Michael Chow'
copyright = '2019, ' + author

linkcheck_ignore = [r'http://localhost:\d+/']

# -- Get version information from Git -------------------------------------

# TODO: use mock to pull version info
try:
    from subprocess import check_output
    release = check_output(['git', 'describe', '--tags', '--always'])
    release = release.decode().strip()
except Exception:
    release = '<unknown>'

# -- Options for HTML output ----------------------------------------------

import alabaster

html_title = project + ' version ' + release

html_theme = 'alabaster'

html_theme_options = {
    #"description": "",
    "github_user": "machow",
    "github_repo": "siuba",
    "fixed_sidebar": False,
    "github_banner": True,
    "github_button": False
    }

# -- nbsphinx customization ---------------------------------------------------

import jupytext


nbsphinx_custom_formats = {
    '.Rmd': lambda s: jupytext.reads(s, '.Rmd'),
}

# hide prompt numbers. we change pd display options in a hidden cell, so it looks
# funny to start at [2]
nbsphinx_prompt_width = 0

# This is processed by Jinja2 and inserted before each notebook
nbsphinx_epilog = r"""
{% set docname = env.doc2path(env.docname, base='docs') %}


.. only:: html

    .. role:: raw-html(raw)
        :format: html

    .. nbinfo::

        Edit page on github `here`__.
        Interactive version:
        :raw-html:`<a href="https://mybinder.org/v2/gh/machow/siuba/{{ env.config.release }}?filepath={{ docname }}"><img alt="Binder badge" src="https://mybinder.org/badge_logo.svg" style="vertical-align:text-bottom"></a>`

    __ https://github.com/machow/siuba/blob/
        {{ env.config.release }}/{{ docname }}

"""


# -- Options for LaTeX output ---------------------------------------------

#latex_elements = {
#    'papersize': 'a4paper',
#    'preamble': r"""
#\usepackage{lmodern}  % heavier typewriter font
#""",
#}
#
#latex_documents = [
#    (master_doc, 'nbsphinx.tex', project, author, 'howto'),
#]
#
#latex_show_urls = 'footnote'
