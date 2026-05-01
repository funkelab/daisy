"""Sphinx configuration for daisy v2."""

from __future__ import annotations

import os
import sys
from importlib.metadata import version as _version
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_REPO))


# -- Project information -----------------------------------------------------

project = "daisy"
author = "William Patton"
copyright = "2026, William Patton"
release = _version("daisy")
version = ".".join(release.split(".")[:2])


# -- General configuration ---------------------------------------------------

extensions = [
    "myst_nb",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
]

templates_path = ["_templates"]
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "**/.ipynb_checkpoints",
    "conf.py",  # source_suffix includes .py; this stops sphinx from treating conf.py as a doc.
]
suppress_warnings = ["myst-nb.lexer"]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "myst-nb",
    ".ipynb": "myst-nb",
    ".py": "myst-nb",
}

master_doc = "index"


# -- MyST / MyST-NB ----------------------------------------------------------

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "fieldlist",
    "tasklist",
    "linkify",
    "smartquotes",
]
myst_heading_anchors = 3

# Notebook execution: cache so unchanged tutorials skip re-execution.
nb_execution_mode = "cache"
nb_execution_timeout = 300
nb_execution_excludepatterns = [
    # Stress test still runs ~100k blocks — too slow for every doc build.
    "tutorials/stress_test.*",
]

# jupytext: read .py files written in py:percent (# %% cells) as notebooks.
nb_custom_formats = {
    ".py": ["jupytext.reads", {"fmt": "py:percent"}],
}


# -- Autodoc / typehints -----------------------------------------------------

autodoc_typehints = "description"
autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
}
autodoc_member_order = "bysource"


# -- Intersphinx -------------------------------------------------------------

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
}


# -- HTML output -------------------------------------------------------------

html_theme = "pydata_sphinx_theme"
html_title = f"daisy v{release}"
html_static_path = ["_static"]
html_theme_options = {
    "github_url": "https://github.com/funkelab/daisy",
    "navbar_align": "left",
    "show_toc_level": 2,
    "icon_links": [
        {
            "name": "PyPI",
            "url": "https://pypi.org/project/daisy/",
            "icon": "fa-brands fa-python",
        },
    ],
}


# -- Symlink examples into tutorials/ at build start -------------------------

def _link_examples(app):
    """Mirror `examples/*.py` into `docs/source/tutorials/` so myst-nb
    discovers them as notebooks (via jupytext py:percent). Symlinks are
    refreshed each build and gitignored under tutorials/."""
    src = _REPO / "examples"
    dst = _HERE / "tutorials"
    dst.mkdir(parents=True, exist_ok=True)
    for py in sorted(src.glob("*.py")):
        link = dst / py.name
        if link.is_symlink() or link.exists():
            link.unlink()
        link.symlink_to(os.path.relpath(py, dst))


def setup(app):
    app.connect("builder-inited", _link_examples)
