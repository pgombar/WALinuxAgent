# Copyright 2020 Microsoft Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Requires Python 2.6+ and Openssl 1.0+
#

import datetime
import os
import threading
import time

import azurelinuxagent.common.logger as logger
from azurelinuxagent.common.event import elapsed_milliseconds, add_event, WALAEventOperation
from azurelinuxagent.common.future import ustr
from azurelinuxagent.common.logcollector import LogCollector
from azurelinuxagent.common.protocol.util import get_protocol_util
from azurelinuxagent.common.version import AGENT_NAME, CURRENT_VERSION


def get_log_upload_handler():
    return LogUploadHandler()


class LogUploadHandler(object):
    LOG_UPLOAD_PERIOD = datetime.timedelta(hours=1)

    def __init__(self):
        self.last_log_upload_time = None
        self.protocol = None
        self.protocol_util = None
        self.thread = None

        self.should_run = True

    def run(self):
        self.start(init_data=True)

    def stop(self):
        self.should_run = False
        if self.is_alive():
            self.thread.join()

    def init_protocols(self):
        # The initialization of ProtocolUtil for the log upload thread should be done within the thread itself rather
        # than initializing it in the ExtHandler thread. This is done to avoid any concurrency issues as each
        # thread would now have its own ProtocolUtil object as per the SingletonPerThread model.
        self.protocol_util = get_protocol_util()
        self.protocol = self.protocol_util.get_protocol()

    def is_alive(self):
        return self.thread is not None and self.thread.is_alive()

    def start(self, init_data=False):
        self.thread = threading.Thread(target=self.daemon, args=(init_data,))
        self.thread.setDaemon(True)
        self.thread.setName("LogUploadHandler")
        self.thread.start()

    def daemon(self, init_data=False):
        if init_data:
            self.init_protocols()

        while self.should_run:
            try:
                if self.last_log_upload_time is None:
                    self.last_log_upload_time = datetime.datetime.utcnow() - LogUploadHandler.LOG_UPLOAD_PERIOD

                if datetime.datetime.utcnow() >= (self.last_log_upload_time + LogUploadHandler.LOG_UPLOAD_PERIOD):
                    self.protocol.update_host_plugin_from_goal_state()
                    self.collect_and_upload_logs()
                    self.last_log_upload_time = datetime.datetime.utcnow()
            except Exception as e:
                logger.warn("An error occurred in the log upload thread main loop; "
                            "will skip the current iteration.\n{0}", ustr(e))

            time.sleep(LogUploadHandler.LOG_UPLOAD_PERIOD.seconds)

    def collect_and_upload_logs(self):
        start_time = datetime.datetime.utcnow()
        try:
            log_collector = LogCollector("bla")  # TODO: reference manifest
            archive_path = log_collector.collect_logs()

            duration = elapsed_milliseconds(start_time)
            archive_size = os.path.getsize(archive_path)

            msg = "Successfully collected logs. Archive size: {0}b, elapsed time: {0} ms".format(archive_size, duration)
            logger.info(msg)
            add_event(
                name=AGENT_NAME,
                version=CURRENT_VERSION,
                op=WALAEventOperation.LogUpload,
                is_success=True,
                message=msg,
                log_event=False)

        except Exception as e:
            duration = elapsed_milliseconds(start_time)

            msg = "Failed to collect logs. Elapsed time: {0} ms. Error: {1}".format(duration, ustr(e))
            logger.warn(msg)
            add_event(
                name=AGENT_NAME,
                version=CURRENT_VERSION,
                op=WALAEventOperation.LogUpload,
                is_success=False,
                message=msg,
                log_event=False)
            return

        self.protocol.put_vm_logs()

    def _invoke_command_with_limits(self, command):
        # README: https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/7/html/resource_
        # management_guide/sec-modifying_control_groups

        # systemd-run --scope --unit=bla
        # --property=CPUAccounting=1 --property=CPUQuota=20%
        # --property=MemoryAccounting=1 --property=MemoryLimit=100M
        # echo 42
        # CPUQuota available since systemd 213: https://github.com/systemd/systemd/blob/master/NEWS

        # Persistent unit for reporting resource usage? Or existing track cgroups?

        # cat cpuhog.sh
        # #!/usr/bin/env bash
        # dd if=/dev/zero of=/dev/null
        pass
