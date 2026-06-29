# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Unit tests for the VST Storage Management Service API.

Tests: storage size, info, version, help, configuration, file list, protected files.
"""
import logging

import pytest
from pytest_bdd import scenarios, given, when, then

from ..unit_test_utils import (
    UnitTestContext,
    api_get,
    api_delete,
    validate_json_response,
    validate_list_response,
    validate_string_response,
    validate_help_response,
    validate_dict_response,
)

logger = logging.getLogger(__name__)

scenarios("../../../features/unit_tests/storage_management/storage_management_api.feature")


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------

@given("the VST storage management API is accessible")
def storage_api_accessible(api_config: dict) -> None:
    assert api_config["base_url"], "Base URL must be configured"


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------

@when("I request the total storage size")
def request_storage_size(context: UnitTestContext, api_config: dict, unit_test_params: dict) -> None:
    timeout = unit_test_params.get("timeout", 30)
    context.response = api_get(
        api_config["base_url"],
        "/vst/api/v1/storage/size",
        verify_ssl=api_config.get("verify_ssl", False),
        timeout=timeout,
    )


@when("I request the storage info")
def request_storage_info(context: UnitTestContext, api_config: dict, unit_test_params: dict) -> None:
    timeout = unit_test_params.get("timeout", 30)
    context.response = api_get(
        api_config["base_url"],
        "/vst/api/v1/storage/info",
        verify_ssl=api_config.get("verify_ssl", False),
        timeout=timeout,
    )


@when("I request the storage management service version")
def request_storage_version(context: UnitTestContext, api_config: dict, unit_test_params: dict) -> None:
    timeout = unit_test_params.get("timeout", 30)
    context.response = api_get(
        api_config["base_url"],
        "/vst/api/v1/storage/version",
        verify_ssl=api_config.get("verify_ssl", False),
        timeout=timeout,
    )


@when("I request the storage management service help")
def request_storage_help(context: UnitTestContext, api_config: dict, unit_test_params: dict) -> None:
    timeout = unit_test_params.get("timeout", 30)
    context.response = api_get(
        api_config["base_url"],
        "/vst/api/v1/storage/help",
        verify_ssl=api_config.get("verify_ssl", False),
        timeout=timeout,
    )


@when("I request the storage management service configuration")
def request_storage_configuration(context: UnitTestContext, api_config: dict, unit_test_params: dict) -> None:
    timeout = unit_test_params.get("timeout", 30)
    context.response = api_get(
        api_config["base_url"],
        "/vst/api/v1/storage/configuration",
        verify_ssl=api_config.get("verify_ssl", False),
        timeout=timeout,
    )


@when("I request the list of all media files")
def request_file_list(context: UnitTestContext, api_config: dict, unit_test_params: dict) -> None:
    timeout = unit_test_params.get("timeout", 30)
    context.response = api_get(
        api_config["base_url"],
        "/vst/api/v1/storage/file/list",
        verify_ssl=api_config.get("verify_ssl", False),
        timeout=timeout,
    )


@when("I request the protected file list")
def request_protected_files(context: UnitTestContext, api_config: dict, unit_test_params: dict) -> None:
    timeout = unit_test_params.get("timeout", 30)
    context.response = api_get(
        api_config["base_url"],
        "/vst/api/v1/storage/file/protected",
        verify_ssl=api_config.get("verify_ssl", False),
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------

@then("the storage response status is 200")
def check_storage_status_200(context: UnitTestContext) -> None:
    assert context.response.status_code == 200, (
        f"Expected 200, got {context.response.status_code}: {context.response.text[:500]}"
    )


@then("the storage info contains total used and available fields")
def check_storage_info_fields(context: UnitTestContext) -> None:
    data = validate_dict_response(context.response)
    expected_fields = ["total", "used", "available"]
    for field in expected_fields:
        assert field in data, f"Missing field '{field}' in storage info: {list(data.keys())}"
    logger.info("Storage: total=%s, used=%s, available=%s",
                data.get("total"), data.get("used"), data.get("available"))


@then("the storage response is a valid version string")
def check_storage_version_string(context: UnitTestContext) -> None:
    version = validate_string_response(context.response)
    assert len(version) > 0, "Version string is empty"
    logger.info("Service version: %s", version)


@then("the storage response is a list of supported API paths")
def check_storage_help_list(context: UnitTestContext) -> None:
    data = validate_help_response(context.response)
    logger.info("Supported APIs: %d", len(data))


@then("the storage response contains configuration fields")
def check_storage_configuration_fields(context: UnitTestContext) -> None:
    data = validate_json_response(context.response)
    assert isinstance(data, dict), "Configuration must be a JSON object"
    assert len(data) > 0, "Configuration is empty"
    logger.info("Configuration has %d fields", len(data))


# ---------------------------------------------------------------------------
# Regression for NVBug 6141778: Delete Videos time range validation.
#
# DELETE /vst/api/v1/storage/file/{streamId} accepted a reversed time range
# (startTime > endTime) and returned HTTP 200 with {"spaceSaved": 0}, making an
# invalid destructive request look successful. It must reject a reversed range
# with 400 while still accepting a well-formed forward range. Both probes use a
# future year-2099 window so nothing can ever match and no data is deleted.
# ---------------------------------------------------------------------------

# Stream id from the bug report. Validation of the time range must happen before
# any file/sensor lookup, so the result does not depend on this stream existing.
STREAM_ID = "20250306train-station"

# Future no-data window: nothing can match, so neither request deletes anything.
EARLIER_TIME = "2099-01-01T00:00:00.000Z"
LATER_TIME = "2099-01-01T00:01:00.000Z"


@when("I request to delete videos with a reversed time range")
def delete_reversed_range(context: UnitTestContext, api_config: dict, unit_test_params: dict) -> None:
    timeout = unit_test_params.get("timeout", 30)
    # startTime (LATER) is after endTime (EARLIER) -> invalid input.
    context.response = api_delete(
        api_config["base_url"],
        f"/vst/api/v1/storage/file/{STREAM_ID}",
        params={"startTime": LATER_TIME, "endTime": EARLIER_TIME},
        verify_ssl=api_config.get("verify_ssl", False),
        timeout=timeout,
    )


@when("I request to delete videos with a valid forward time range")
def delete_forward_range(context: UnitTestContext, api_config: dict, unit_test_params: dict) -> None:
    timeout = unit_test_params.get("timeout", 30)
    # startTime (EARLIER) is before endTime (LATER) -> valid; future window deletes nothing.
    context.response = api_delete(
        api_config["base_url"],
        f"/vst/api/v1/storage/file/{STREAM_ID}",
        params={"startTime": EARLIER_TIME, "endTime": LATER_TIME},
        verify_ssl=api_config.get("verify_ssl", False),
        timeout=timeout,
    )


@then("the delete videos response status is 400")
def check_delete_status_400(context: UnitTestContext) -> None:
    # Bug 6141778: this currently returns 200 with {"spaceSaved": 0}.
    assert context.response.status_code == 400, (
        f"A reversed time range (startTime > endTime) must be rejected with "
        f"400 Bad Request, got {context.response.status_code}: "
        f"{context.response.text[:300]}"
    )


@then("the delete videos response status is 200")
def check_delete_status_200(context: UnitTestContext) -> None:
    assert context.response.status_code == 200, (
        f"A valid forward time range must be accepted with 200, got "
        f"{context.response.status_code}: {context.response.text[:300]}"
    )
