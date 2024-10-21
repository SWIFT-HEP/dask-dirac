"""Dask Dirac Template Module"""

from __future__ import annotations

from functools import cache

from jinja2 import Environment, PackageLoader, Template, select_autoescape


@cache
def get_jinja_env() -> Environment:
    """Get the project-specific jinja environment"""
    return Environment(
        loader=PackageLoader("dask_dirac"), autoescape=select_autoescape()
    )


def get_template(template_name: str) -> Template:
    """Attempts to retrieve template named {template_name}"""
    return get_jinja_env().get_template(template_name)
