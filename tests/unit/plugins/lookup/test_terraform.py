from __future__ import (absolute_import, division, print_function)
__metaclass__ = type


import pytest
from ansible_collections.community.general.plugins.lookup.terraform import json_query
import json
MOCKED_DATA = '''{
            "version": 18,
            "terraform_version": "0.12.20",
            "serial": 174,
            "lineage": "4112ahd23-925z-4e9a-1ce3-6026649c1ef7",
            "outputs": {},
            "resources" : [
          {
            "module": "module.sample-module",
            "mode": "data",
            "type": "aws_acm_certificate",
            "name": "my-sample-certificate",
            "provider": "provider.aws",
            "instances": [
                {
                "schema_version": 0,
                "attributes": {
                    "arn": "arn:aws:acm:eu-west-2:12345678:certificate/01986237-8735-40b9-bd0d-954c674f454c",
                    "domain": "example.com",
                    "id": "2020-05-26 19:22:40.6699801 +0000 UTC",
                    "key_types": null,
                    "most_recent": true,
                    "statuses": null,
                    "types": [
                      "AMAZON_ISSUED"
                    ]
                }
                }
            ]
    },
                 {
            "mode": "managed",
            "type": "aws_iam_role_policy",
            "name": "s3-access-policy",
            "provider": "provider.aws",
            "instances": [
              {
                "schema_version": 0,
                "attributes": {
                  "id": "access-role:s3-access",
                  "name": "s3-access",
                  "name_prefix": null,
                  "role": "sample-role"
                },
                "private": "bnVsbA=="
              }
            ]
          }
            ]
            }
'''

class TestTerraformLookup:

    @pytest.fixture(scope="module")
    def convert_mocked_data(self):
        yield json.loads(MOCKED_DATA)

    def test_terraform_version_number(self, convert_mocked_data):
        assert json_query(convert_mocked_data, "terraform_version") == "0.12.20"

        
    def test_sample_certificate_name(self, convert_mocked_data):
        assert json_query(convert_mocked_data,"resources[?module == 'module.sample-module'].name") == ["my-sample-certificate"]

    
    def test_schema_version(self, convert_mocked_data):
        assert json_query(convert_mocked_data, "resources[?instances[?attributes.name == 's3-access']].instances[].attributes.name") == ['s3-access']

