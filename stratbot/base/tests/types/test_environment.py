from __future__ import annotations

from typing import Literal, get_args, get_origin

from stratbot.base.types.environment import Environment, EnvironmentType


def test_literal_type_and_enum_consistent():
    assert get_origin(EnvironmentType) is Literal
    args = sorted(get_args(EnvironmentType))
    values = sorted([*Environment])
    assert args == values
    assert tuple(args) == tuple(values)
    assert sorted(set(args)) == args
    assert sorted(set(values)) == args
    for arg, value in zip(args, values, strict=True):
        assert arg == value
        assert arg == Environment(value)


def test_environment_enum_properties():
    dev = Environment.DEV
    test = Environment.TEST
    ci = Environment.CI
    stage = Environment.STAGE
    prod = Environment.PROD

    assert dev.is_dev
    assert not dev.is_test
    assert not dev.is_ci
    assert not dev.is_stage
    assert not dev.is_prod
    assert not dev.is_running_tests

    assert not test.is_dev
    assert test.is_test
    assert not test.is_ci
    assert not test.is_stage
    assert not test.is_prod
    assert test.is_running_tests

    assert not ci.is_dev
    assert not ci.is_test
    assert ci.is_ci
    assert not ci.is_stage
    assert not ci.is_prod
    assert ci.is_running_tests

    assert not stage.is_dev
    assert not stage.is_test
    assert not stage.is_ci
    assert stage.is_stage
    assert not stage.is_prod
    assert not stage.is_running_tests

    assert not prod.is_dev
    assert not prod.is_test
    assert not prod.is_ci
    assert not prod.is_stage
    assert prod.is_prod
    assert not prod.is_running_tests
