# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'dataplug'
copyright = '2023, Cloudlab URV'
author = 'Cloudlab URV'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'myst_parser',
    'sphinx.ext.autodoc',
    'sphinx.ext.todo',
    'sphinx_copybutton',
    'nbsphinx'
]

todo_include_todos = True
nbsphinx_allow_errors = False

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', 'README.md', 'Dockerfile']

source_suffix = {
    '.rst': 'restructuredtext',
    '.txt': 'markdown',
    '.md': 'markdown',
}

# -- Autodoc options ---------------------------------------------------------

autodoc_typehints = 'description'

# -- nbsphinx options --------------------------------------------------------

jupyter_execute_notebooks = 'never'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
html_static_path = ['_static']

html_logo = "_static/lithops_logo_readme.png"
html_favicon = '_static/favicon.png'

html_theme_options = {
    "source_repository": "https://github.com/pradyunsg/furo/",
    "source_branch": "main",
    "source_directory": "docs/",
    "light_css_variables": {
        "color-brand-primary": "#7C4DFF",
        "color-brand-content": "#7C4DFF",
        "font-stack": "Tahoma, sans-serif",
        "font-stack--monospace": "Consolas, monospace",
    },

}
