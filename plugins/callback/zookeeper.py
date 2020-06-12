# (C) 2020, Turntabl, <turtabl.io>
# (c) 2020 Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = '''
    callback: connects to zookeeper and creates a lock
    type: locking
    short_description: acquire and release a lock
    description:
      - This callback acquires and releases a lock per each run
    requirements:
     - Whitelist in configuration
     - A writeable /var/log/ansible/hosts directory by the user executing Ansible on the controller
    options:
      hosts:
        description: hosts to connect to.
        ini:
          - section: callback_zookeeper
            key: hosts
        required: True
        type: str
      user_id:
        description: identifier for the lock
        env:
          - name: USER_ID
        required: True
        type: str
      lockpath:
        description: path to create lock nodes
        env: 
          - name: LOCK_PATH
        ini:
          - section: callback_zookeeper
            key: lockpath
        required: True
        type: path
'''

EXAMPLES = '''
examples: >
  To enable, add this to your ansible.cfg file in the defaults block
    [defaults]
    callback_whitelist = zookeeper
  Set the environment variable
    export USER_ID=doreen7555
    export LOCKPATH=/ansible-zoo
  Set the ansible.cfg variable in the callback_splunk block
    [callback_zookeeper]
    hosts = 127.0.0:2181
    lockpath = /default
'''

import time
import sys
import os
from ansible import constants as C
from ansible.module_utils._text import to_bytes
from ansible.module_utils.common._collections_compat import MutableMapping
from ansible.parsing.ajson import AnsibleJSONEncoder
from ansible.plugins.callback import CallbackBase
from ansible.errors import AnsibleError
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import LockTimeout


class CallbackModule(CallbackBase):

    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'aggregate'
    CALLBACK_NAME = 'zookeeper'
    CALLBACK_NEEDS_WHITELIST = True

    def __init__(self):
        super(CallbackModule, self).__init__()


    def set_options(self, task_keys=None, var_options=None, direct=None):
        super(CallbackModule, self).set_options(task_keys=task_keys, var_options=var_options, direct=direct)
        self.hosts = self.get_option("hosts")
        self.identifier = os.environ.get("USER_ID")
        self.lock_path = os.environ.get("LOCK_PATH")
        if self.lock_path:
            self.locknode = self.lock_path
        else:
            self.locknode = self.get_option("lockpath")
        self.client = KazooClient(self.hosts)
        self.lock = self.client.Lock(self.locknode, self.identifier)


    def my_listener(self, state):
        self.state = state
        if self.state == KazooState.LOST:
            msg = "Session expired!"
            self._display.display(msg, color=C.COLOR_ERROR, stderr=False)
            sys.exit()
        elif self.state == KazooState.SUSPENDED:
            msg = "Connection issues!"
            self._display.display(msg, color=C.COLOR_ERROR, stderr=False)
            sys.exit()
        else:
            msg = "Connection successful!"
            self._display.display(msg, color=C.COLOR_OK, stderr=False)


    def v2_playbook_on_start(self, playbook):
        try:
            self.client.add_listener(self.my_listener)
            self.client.start()
        except:
            msg="[ERROR] Connection closed/timeout/loss"
            self._display.display(msg, color=C.COLOR_ERROR, stderr=False)
            sys.exit(0)
        try:
            self.lock.acquire()
        except LockTimeout:
            msg="[ERROR] Lock already acquired"
            self._display.display(msg, color=C.COLOR_ERROR, stderr=False)
            sys.exit(0)

        msg="Lock acquired!"
        self._display.display(msg, color=C.COLOR_OK, stderr=False)
    

    def v2_playbook_on_stats(self, stats):
        self.lock.release()
        msg="Lock released!"
        self._display.display(msg, color=C.COLOR_OK, stderr=False)
        self.client.stop()
        self.client.add_listener(self.my_listener)

