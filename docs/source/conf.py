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
    'sphinxawesome_theme'
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

html_theme = "sphinxawesome_theme"
html_static_path = ['images']

html_logo = "images/lithops_logo_readme.png"
html_favicon = 'images/favicon.png'

# html_theme_options = {
#     "source_repository": "https://github.com/CLOUDLAB-URV/dataplug",
#     "source_branch": "main",
#     "source_directory": "docs/",
#     "light_css_variables": {
#         "color-brand-primary": "#EA5455",
#         "color-brand-content": "#EA5455",
#         "font-stack": "Tahoma, sans-serif",
#         "font-stack--monospace": "Consolas, monospace",
#     },
#
# }
