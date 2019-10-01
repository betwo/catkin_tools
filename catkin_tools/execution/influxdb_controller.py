# Copyright 2016 Open Source Robotics Foundation, Inc.
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

try:
    # Python3
    from queue import Empty
except ImportError:
    # Python2
    from Queue import Empty

import threading

try:
    from influxdb import InfluxDBClient
    have_influx_db = True
except ImportError:
    have_influx_db = False


class InfluxDBStatusController(threading.Thread):
    def __init__(
            self,
            event_queue,
            database,
            host, port, username, password):
        """
        :param event_queue: The event queue used by an Executor
        :param database: InfluxDB data base
        :param host: InfluxDB hostname
        :param port: InfluxDB port
        :param username: InfluxDB username
        :param password: InfluxDB password
        """
        super(InfluxDBStatusController, self).__init__()

        self.event_queue = event_queue

        self.keep_running = True

        if have_influx_db:
            self.metric_client = InfluxDBClient(
                host=host, port=port, username=username, password=password)
            self.metric_client.switch_database(database)
        else:
            self.metric_client = None

    def run(self):
        if self.metric_client is None:
            return

        queued_jobs = []
        active_jobs = []
        completed_jobs = {}

        while True:
            # Check if we should stop
            if not self.keep_running:
                break

            # Try to get an event from the queue (blocking)
            try:
                event = self.event_queue.get(True)
            except Empty:
                break

            # A `None` event is a signal to terminate
            if event is None:
                break

            # Handle the received events
            if 'JOB_STATUS' == event.event_id:
                queued_jobs = event.data['queued']
                active_jobs = event.data['active']
                completed_jobs = event.data['completed']
                pending_jobs = event.data['pending']

                json_body = [
                    {
                        "measurement": "catkin_build",
                        "fields": {
                            "queued": len(queued_jobs),
                            "active": len(active_jobs),
                            "completed": len(completed_jobs),
                            "pending": len(pending_jobs)
                        }
                    }
                ]
                try:
                    self.metric_client.write_points(json_body)
                except Exception as e:
                    print("Cannot send metrics:", e)
                    pass

                if all([len(event.data[t]) == 0 for t in ['pending', 'queued', 'active']]):
                    break

        self.metric_client.close()
