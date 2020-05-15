import json

import pytest

import sys

from ansible_collections.community.general.plugins.modules.cloud.misc import terraform
import terraform

import mock
import os

from ansible.module_utils._text import to_bytes
from ansible.module_utils import basic


def set_module_args(args):
    args = json.dumps({'ANSIBLE_MODULE_ARGS':args})
    basic._ANSIBLE_ARGS = to_bytes(args)



class AnsibleExitJson(Exception):
    pass


class AnsibleFailJson(Exception):
    pass


def fail_json(*args, **kwargs):
    kwargs['failed'] = True
    raise AnsibleFailJson(kwargs)
    


def exit_json(*args, **kwargs):
    if 'changed' not in kwargs:
        kwargs['changed'] = False

    raise AnsibleExitJson(kwargs)


module = mock.MagicMock()
module.fail_json.side_effect = AnsibleFailJson(Exception)
module.exit_json.side_effect = AnsibleExitJson(Exception)

def test_state_args():
    with pytest.raises(AnsibleFailJson):
        terraform._state_args(module, '/rrrr')
    module.fail_json.assert_called()

def test_returned_value_state_args():
    value = terraform._state_args(module, '/vagrant')
    assert value == ['-state', '/vagrant']

def test_return_empty_list_state_args():
    value = terraform._state_args(module, '')
    assert value == []
    

def test_fail_json_preflight_validation_with_project_path_not_provided():
    with pytest.raises(AnsibleFailJson):
        terraform.preflight_validation(module,'', '/rri')
    print(module.fail_json.msg)

    module.fail_json.assert_called()
    

def test_preflight_validation_with_arguments_satisfied():
    set_module_args({
        'project_path':'/vagrant',
        'bin_path':'/vagrant',
        })
    module.patch.object(basic.AnsibleModule, 'run_command', return_values=(0, '', ''))
    module.run_command('/usr/bin/command args')
    #with mock.patch.object(basic.AnsibleModule, 'run_command') as run_command:
       # run_command.return_value = 0, '', ''
        #with pytest.raises(AnsibleExitJson) as result:
            #terraform.preflight_validation(module, '/vagrant', '/vagrant', ['yy','see'])
    module.main()
    module.run_command.assert_called_once_with('/usr/bin/command args')


def test_terraform_without_argument(capfd):
    set_module_args({})
    with pytest.raises(SystemExit) as results:
        terraform.main()

    out, err = capfd.readouterr()
    assert not err
    assert json.loads(out)['failed']
    assert 'project_path' in json.loads(out)['msg']

