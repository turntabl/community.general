# (c) 2020, Turntabl GH <info(at)turntabl.io>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type


DOCUMENTATION = '''
lookup: terraform
description:
  - This lookup returns the ids of resources from state plan files
short_description: Return resource ids from terraform statefiles by passing either the resource type or resource name or both
author: Turntabl GH <info@turntabl.io>
requirements:
  - jmespath
options:
  state_file:
    description:
      - The path to the statefile.
    required: true
  resource_type:
    description:
      - The terraform resource type.
    required: false
  resource_name:
    description:
      - The terraform resource name.
    required: false
'''

EXAMPLES = '''
- debug: msg="the ID is {{ lookup('terraform',
    resource_type='aws_lb', state_file='mydir/foo.txt', resource_name='foo') }}"
'''

RETURN = '''
_raw:
    description:
        - IDs about resources which match lookup
'''


from ansible.plugins.lookup import LookupBase
from ansible.module_utils._text import to_native
from ansible.errors import AnsibleError
import json


try:
    import jmespath
    HAS_LIB = True
except ImportError:
    HAS_LIB = False


def json_query(data, expr):

    '''Query json data using jmespath query language ( http://jmespath.org )'''
    if not HAS_LIB:
        raise AnsibleError('You need to install "jmespath" to run this plugin')
    try:
        return jmespath.search(expr, data)
    except jmespath.exceptions.JMESPathError as e:
        raise AnsibleError('JMESPathError in json_query lookup plugin:\n%s' % to_native(e))
    except Exception as e:
        raise AnsibleError('Error in jmespath.search in json_query lookup plugin:\n%s' % to_native(e))


class LookupModule(LookupBase):

    def run(self, terms, variables=None, **kwargs):

        returned_data = []

        state_file = kwargs.get('state_file')
        resource_type = kwargs.get('resource_type', None)
        resource_name = kwargs.get('resource_name', None)

        lookupfile = self.find_file_in_search_path(variables, 'files', state_file)

        try:
            if lookupfile:
                content, show_data = self._loader._get_file_contents(lookupfile)
                data = json.loads(content)
                query = ""
                if not resource_type and not resource_name:
                    raise AnsibleError("Pass at least one of the options 'resource_type' or 'resource_name'")
                if resource_type is not None and resource_name is None:
                    query = "resources[?type == '{resource_type}'].instances[].attributes.id".format(resource_type=resource_type)
                if resource_type is None and resource_name is not None:
                    query = "resources[?name == '{resource_name}'].instances[].attributes.id".format(resource_name=resource_name)
                if resource_type is not None and resource_name is not None:
                    query = ("((resources[?type == '{resource_type}'].instances[].attributes.id)"
                             " && (resources[?name == '{resource_name}'].instances[]."
                             "attributes.id))".format(resource_type=resource_type, resource_name=resource_name))

                returned_data.append(json_query(data, query))
        except AnsibleError:
            raise AnsibleError('File not found in %s' % state_file)
        return returned_data
