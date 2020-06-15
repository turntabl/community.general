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
      locknode:
        description: path to create lock nodes
        env: 
          - name: LOCK_NODE
        required: False
        type: str
        default: playbook name + _ + inventory name
'''

EXAMPLES = '''
examples: >
  To enable, add this to your ansible.cfg file in the defaults block
    [defaults]
    callback_whitelist = zookeeper
  Set the environment variable
    export USER_ID=doreen7555
    export LOCK_NODE=ansible-zoo
  Set the ansible.cfg variable in the callback_zookeeper block
    [callback_zookeeper]
    hosts = 127.0.0:2181
'''

import time
import sys
import os
import re
from ansible import constants as C
from ansible import context
from ansible.module_utils.common._collections_compat import MutableMapping
from ansible.parsing.ajson import AnsibleJSONEncoder
from ansible.plugins.callback import CallbackBase
from kazoo.client import KazooClient, KazooState
from os.path import basename

try:
    from time import monotonic as now
except ImportError:
    from time import time as now

import six

from kazoo.exceptions import (
    CancelledError,
    KazooException,
    LockTimeout,
    NoNodeError,
)
from kazoo.retry import (
    ForceRetryError,
    KazooRetry,
    RetryFailedError,
)


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
        self.client = KazooClient(self.hosts)


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
        self.locknode_env = os.environ.get("LOCK_NODE")
        self.playbook_name = playbook._file_name.split('.')[0]
        # setting playbook name with host names
        for argument in (a for a in context.CLIARGS if a != 'args'):
            val = context.CLIARGS[argument]
            if argument == 'inventory':
                for i in val:
                    hosts = '_' + basename(i)
                    self.playbook_name += hosts
        if self.locknode_env:
            self.lock_node = self.locknode_env
        else:
            self.lock_node = self.playbook_name

        self.lock = Lock(self.client, self.lock_node, self.identifier)
        self.set_option('lock_object', self.lock)
        self.lock_object = self.get_option('lock_object')
        try:
            self.client.add_listener(self.my_listener)
            self.client.start()
        except:
            msg="[ERROR] Connection closed/timeout/loss"
            self._display.display(msg, color=C.COLOR_ERROR, stderr=False)
            sys.exit(0)
        try:
            self.lock_object.acquire()
        except LockTimeout:
            msg="[ERROR] Lock already acquired"
            self._display.display(msg, color=C.COLOR_ERROR, stderr=False)
            sys.exit(0)

        msg="Lock acquired!"
        self._display.display(msg, color=C.COLOR_OK, stderr=False)


    def v2_playbook_on_stats(self, stats):
        self.lock_object.release()
        msg="Lock released!"
        self._display.display(msg, color=C.COLOR_OK, stderr=False)
        self.client.stop()
        self.client.add_listener(self.my_listener)


#Kazoo Lock recipie
class Lock(object):
    _NODE_NAME = "__lock__"
    _EXCLUDE_NAMES = ["__lock__"]

    def __init__(self, client, path, identifier=None, extra_lock_patterns=()):
        self.locknode = path
        self.client = client
        self.path = "/" + path
        self._exclude_names = set(
            self._EXCLUDE_NAMES + list(extra_lock_patterns)
        )
        self._contenders_re = re.compile(
            r"(?:{patterns})(-?\d{{10}})$".format(
                patterns="|".join(self._exclude_names)
            )
        )
        self.data = str(identifier or "").encode("utf-8")
        self.node = None
        self.wake_event = client.handler.event_object()
        self.prefix = self.locknode + self._NODE_NAME
        self.create_path = self.path + "/" + self.prefix

        self.create_tried = False
        self.is_acquired = False
        self.assured_path = False
        self.cancelled = False
        self._retry = KazooRetry(
            max_tries=None, sleep_func=client.handler.sleep_func
        )
        self._lock = client.handler.lock_object()

    def _ensure_path(self):
        self.client.ensure_path(self.path)
        self.assured_path = True

    def cancel(self):
        """Cancel a pending lock acquire."""
        self.cancelled = True
        self.wake_event.set()

    def acquire(self, blocking=True, timeout=None, ephemeral=True):
        def _acquire_lock():
            got_it = self._lock.acquire(False)
            if not got_it:
                raise ForceRetryError()
            return True

        retry = self._retry.copy()
        retry.deadline = timeout

        # Ensure we are locked so that we avoid multiple threads in
        # this acquistion routine at the same time...
        locked = self._lock.acquire(False)
        if not locked and not blocking:
            return False
        if not locked:
            # Lock acquire doesn't take a timeout, so simulate it...
            # XXX: This is not true in Py3 >= 3.2
            try:
                locked = retry(_acquire_lock)
            except RetryFailedError:
                return False
        already_acquired = self.is_acquired
        try:
            gotten = False
            try:
                gotten = retry(
                    self._inner_acquire,
                    blocking=blocking,
                    timeout=timeout,
                    ephemeral=ephemeral,
                )
            except RetryFailedError:
                pass
            except KazooException:
                # if we did ultimately fail, attempt to clean up
                exc_info = sys.exc_info()
                if not already_acquired:
                    self._best_effort_cleanup()
                    self.cancelled = False
                six.reraise(exc_info[0], exc_info[1], exc_info[2])
            if gotten:
                self.is_acquired = gotten
            if not gotten and not already_acquired:
                self._best_effort_cleanup()
            return gotten
        finally:
            self._lock.release()

    def _watch_session(self, state):
        self.wake_event.set()
        return True

    def _inner_acquire(self, blocking, timeout, ephemeral=True):

        # wait until it's our chance to get it..
        if self.is_acquired:
            if not blocking:
                return False
            raise ForceRetryError()

        # make sure our election parent node exists
        if not self.assured_path:
            self._ensure_path()

        node = None
        if self.create_tried:
            node = self._find_node()
        else:
            self.create_tried = True

        if not node:
            node = self.client.create(
                self.create_path, self.data, ephemeral=ephemeral, sequence=True
            )
            # strip off path to node
            node = node[len(self.path) + 1:]

        self.node = node

        while True:
            self.wake_event.clear()

            # bail out with an exception if cancellation has been requested
            if self.cancelled:
                raise CancelledError()

            predecessor = self._get_predecessor(node)
            if predecessor is None:
                return True

            if not blocking:
                return False

            # otherwise we are in the mix. watch predecessor and bide our time
            predecessor = self.path + "/" + predecessor
            self.client.add_listener(self._watch_session)
            try:
                self.client.get(predecessor, self._watch_predecessor)
            except NoNodeError:
                pass  # predecessor has already been deleted
            else:
                self.wake_event.wait(timeout)
                if not self.wake_event.isSet():
                    raise LockTimeout(
                        "Failed to acquire lock on %s after %s seconds"
                        % (self.path, timeout)
                    )
            finally:
                self.client.remove_listener(self._watch_session)

    def _watch_predecessor(self, event):
        self.wake_event.set()

    def _get_predecessor(self, node):
        children = self.client.get_children(self.path)
        found_self = False
        
        contender_matches = []
        for child in children:
            match = self._contenders_re.search(child)
            if match is not None:
                contender_matches.append(match)
            if child == node:
                # Remember the node's match object so we can short circuit
                # below.
                found_self = match

        if found_self is False: 
            raise ForceRetryError()

        predecessor = None
        # Sort the contenders using the sequence number extracted by the regex,
        # then extract the original string.
        for match in sorted(contender_matches, key=lambda m: m.groups()):
            if match is found_self:
                break
            predecessor = match.string

        return predecessor

    def _find_node(self):
        children = self.client.get_children(self.path)
        for child in children:
            if child.startswith(self.prefix):
                return child
        return None

    def _delete_node(self, node):
        self.client.delete(self.path + "/" + node)

    def _best_effort_cleanup(self):
        try:
            node = self.node or self._find_node()
            if node:
                self._delete_node(node)
        except KazooException:  # pragma: nocover
            pass

    def release(self):
        """Release the lock immediately."""
        return self.client.retry(self._inner_release)

    def _inner_release(self):
        if not self.is_acquired:
            return False

        try:
            self._delete_node(self.node)
        except NoNodeError:  # pragma: nocover
            pass

        self.is_acquired = False
        self.node = None
        return True

