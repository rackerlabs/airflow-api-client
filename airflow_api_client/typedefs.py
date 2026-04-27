# Copyright 2026 Rackspace Technology, Inc. SPDX-License-Identifier: Apache-2.0
"""Types used across the application."""


class App:  # pylint: disable=too-few-public-methods
    """Main application class that holds the API client and command-line arguments."""

    def __init__(self, client, args):
        self.client = client
        self.args = args
