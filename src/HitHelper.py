import re
from datetime import datetime, timezone
import logging as lgng

from dateutil import parser
from elasticsearch_dsl.response.hit import Hit


class HitHelper:
    _hit_data = None
    _k8s_data = None
    strange_logs_pods = ['neurodata-api']

    def __init__(self, hit: Hit, logger: lgng):
        self._hit = hit
        self._logger = logger

    def get_k8s_data(self):
        if self._k8s_data is None:
            try:
                self._k8s_data = self._hit.kubernetes.to_dict()
            except AttributeError as e:
                if str(e) == "'Hit' object has no attribute 'kubernetes'":
                    return None
                else:
                    raise AttributeError(str(e))
        return self._k8s_data

    def get_hit_data(self):
        if self._hit_data is None:
            self._hit_data = self._hit.to_dict()
        return self._hit_data

    def get_k8s_date(self):
        return self.get_hit_data().get("@timestamp", None)

    def get_pod_info(self):
        k8s_data = self.get_k8s_data()
        if k8s_data is None:
            return None, None

        pod_hash = k8s_data.get('pod_name', None)
        labels = k8s_data.get('labels', None)
        if labels is None:
            return None, None

        pod_name = k8s_data.get('labels').get('app', None)
        if pod_name is not None and pod_hash is not None:
            pod_hash = pod_hash.replace(f'{pod_name}-', '')
        return pod_name, pod_hash

    def get_log(self):
        return self._hit.log

    def get_event_id(self, event_id_regex):
        if event_id_regex is None:
            return None

        regex_result = re.search(event_id_regex, self.get_log())
        if regex_result:
            return regex_result.group(1)
        else:
            return None

    def get_timestamp(self, pod_name):
        if pod_name in self.strange_logs_pods:
            regex = '^.(\d{4}\s\d{2}:\d{2}:\d{2}\.\d{6})\s'
            fix_date = True
        else:
            regex = '^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\s[-+]\d{4})\s'
            fix_date = False

        regex_result = re.search(regex, self.get_log())
        if regex_result:
            if fix_date:
                group = f'{datetime.now().year}-{regex_result.group(1)[:2]}-{regex_result.group(1)[2:]}'
                timestamp = parser.parse(group)
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            else:
                timestamp = parser.parse(regex_result.group(1))
            return timestamp
        else:
            return None

    def get_object_type(self, pod_name):
        if pod_name not in self.strange_logs_pods:
            return None

        regex = '\[dtype\:\s([a-z]{1,10})\]'
        regex_result = re.search(regex, self.get_log())
        if regex_result:
            data_type = regex_result.group(1)
            if data_type == 'video':
                return 'V'
            elif data_type == 'images':
                return 'I'
            self._logger.error(f'Wrong dtype: {data_type} in log:{self.get_log()}')
            return None
        return None
