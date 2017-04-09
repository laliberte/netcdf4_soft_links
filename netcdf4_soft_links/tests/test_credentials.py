from netcdf4_soft_links import certificates
from argparse import Namespace
import getpass
import select
import sys
import pytest


def test_no_creds():
    options = Namespace()
    options2 = certificates.prompt_for_username_and_password(options)
    assert options2 == options


def test_both_creds():
    options = Namespace(openid='test', password='test')
    options2 = certificates.prompt_for_username_and_password(options)
    assert options2 == options


def test_openid_no_pass_creds(monkeypatch):
    options = Namespace(openid='test')

    monkeypatch.setattr(getpass, 'getpass', lambda x: "pass")
    options2 = certificates.prompt_for_username_and_password(options)

    expected = Namespace(openid='test', password='pass')

    assert options2 == expected


def test_openid_pass_from_pipe_no_input(monkeypatch):
    options = Namespace(openid='test', password=None,
                        password_from_pipe=True)

    def _mock_select(*x):
        return (True, None, None)

    class _mock_stdin:
        def readline(self):
            return "pass  "

    monkeypatch.setattr(select, 'select', _mock_select)
    monkeypatch.setattr(sys, 'stdin', _mock_stdin())
    options2 = certificates.prompt_for_username_and_password(options)

    expected = Namespace(openid='test', password='pass',
                         password_from_pipe=True)
    assert options2 == expected


def test_openid_pass_from_pipe(monkeypatch):
    options = Namespace(openid='test', password=None,
                        password_from_pipe=True)

    def _mock_select(*x):
        return (False, None, None)

    monkeypatch.setattr(select, 'select', _mock_select)
    with pytest.raises(EnvironmentError):
        certificates.prompt_for_username_and_password(options)


def test_openid_ceda(monkeypatch):
    options = Namespace(openid='https://ceda.ac.uk')

    monkeypatch.setitem(__builtins__, 'raw_input', lambda x: "test_user")
    monkeypatch.setattr(getpass, 'getpass', lambda x: "pass")
    options2 = certificates.prompt_for_username_and_password(options)

    expected = options
    expected.username = 'test_user'

    assert options2 == expected
