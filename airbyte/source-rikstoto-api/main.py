#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_rikstoto_api import SourceRikstotoApi

if __name__ == "__main__":
    source = SourceRikstotoApi()
    launch(source, sys.argv[1:])
