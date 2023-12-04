from __future__ import annotations

import pytest

import dask_dirac.templates as templates


@pytest.fixture
def jinja_env():
    return templates.get_jinja_env()


def test_jdl_template_rendering(jinja_env):
    # Prepare test data
    test_executable_args = "test_args"
    test_owner_group = "test_group"
    test_dirac_site = "test_site"

    # Load the template
    template = jinja_env.get_template("jdl.j2")

    # Render the template
    result = template.render(
        executable_args=test_executable_args,
        owner=test_owner_group,
        dirac_site=test_dirac_site,
    )

    # Assertions
    assert 'Arguments = "test_args"' in result
    assert "OwnerGroup = test_group" in result
    assert 'Site = "test_site"' in result

    # Test without dirac_site
    result_no_site = template.render(
        executable_args=test_executable_args, owner=test_owner_group, dirac_site=None
    )
    assert "Site =" not in result_no_site
