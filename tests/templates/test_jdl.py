from __future__ import annotations

import pytest

import dask_dirac.templates as templates


@pytest.fixture
def jinja_env():
    return templates.get_jinja_env()


def test_jdl_template_rendering(jinja_env):
    # Prepare test data
    test_executable_args = "test_args"
    test_container = "python:3.11"
    test_public_address = "127.0.0.1"
    test_owner_group = "test_group"
    test_dirac_site = "test_site"

    # Load the template
    template = jinja_env.get_template("jdl.j2")

    # Render the template
    result = template.render(
        container=test_container,
        public_address=test_public_address,
        owner=test_owner_group,
        dirac_site=test_dirac_site,
    )

    # Assertions
    assert test_container in result
    assert test_public_address in result
    assert "OwnerGroup = test_group" in result
    assert 'Site = "test_site"' in result

    # Test without dirac_site
    result_no_site = template.render(
        executable_args=test_executable_args, owner=test_owner_group
    )
    assert "Site =" not in result_no_site

    # test with extra_args
    test_extra_args = "--nooop"
    result_extra_args = template.render(
        executable_args=test_executable_args,
        owner=test_owner_group,
        extra_args=test_extra_args,
    )
    assert test_extra_args in result_extra_args


def test_submit_command_template_rendering(jinja_env):
    # Prepare test data
    test_submission_url = "test_url"
    test_jdl_file = "test_jdl_file"
    test_cert_path = "test_cert_path"
    test_user_proxy = "test_user_proxy"

    # Load the template
    template = jinja_env.get_template("submit_command.j2")

    # Render the template
    result = template.render(
        submission_url=test_submission_url,
        jdl_file=test_jdl_file,
        cert_path=test_cert_path,
        user_proxy=test_user_proxy,
    )

    # Assertions
    expected_command = "dask-dirac submit test_url test_jdl_file --capath test_cert_path --user-proxy test_user_proxy --dask-script"
    assert result.strip() == expected_command
