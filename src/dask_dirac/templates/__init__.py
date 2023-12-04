from __future__ import annotations

from jinja2 import Environment, PackageLoader, Template, select_autoescape

env = None


def get_jinja_env() -> Environment:
    global env
    if env is None:
        env = Environment(
            loader=PackageLoader("dask_dirac"), autoescape=select_autoescape()
        )
    return env


def get_template(template_name: str) -> Template:
    return get_jinja_env().get_template(template_name)
