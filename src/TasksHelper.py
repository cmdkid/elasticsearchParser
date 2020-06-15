import logging as lgng
import json
import pickle

from rules import rules, rules_pods_list


class TasksHelper:
    _prepared_data = None

    def __init__(self, uncompleted_tasks, uncompleted_tasks_cols):
        self._tasks = dict()
        self._uncompleted_tasks = None
        self._completed_tasks = None
        self.load_uncompleted_tasks(uncompleted_tasks, uncompleted_tasks_cols)

    @staticmethod
    def _db_line_to_dict(db_line, columns_list):
        dict_item = dict()
        for idx, key in enumerate(columns_list):
            dict_item.setdefault(key, db_line[idx])
        return dict_item

    def _add_db_data_list(self, db_data_list: list, db_data_columns: list):
        for db_line in db_data_list:
            self.add_data(self._db_line_to_dict(db_line, db_data_columns))

    def add_data(self, data: dict):
        pod_name = data.get('pod_name', None)
        if pod_name not in self._tasks.keys():
            self._tasks.setdefault(pod_name, dict())

        rule_type = data.get('rule_type', None)
        if rule_type not in self._tasks.get(pod_name, {}).keys():
            self._tasks.get(pod_name).setdefault(rule_type, dict())

        rule_id = data.get('rule_id', None)
        if rule_id not in self._tasks.get(pod_name, {}).get(rule_type, {}).keys():
            self._tasks.get(pod_name).get(rule_type).setdefault(rule_id, list())

        self._tasks.get(pod_name).get(rule_type).get(rule_id).append(data)

    def load_tasks_obj(self, file_path):
        with open(file_path, "rb") as file:
            self._tasks = pickle.load(file)

    def _unc_col_id_by_name(self, col_name):
        for idx, val in enumerate(self._uncompleted_tasks_cols):
            if col_name == val:
                return idx
        return None

    def _get_uncompleted_event_ids_and_pod_hashes(self):
        event_ids = dict()
        pod_hashes = dict()
        event_id_id = self._unc_col_id_by_name('event_id')
        pod_hash_id = self._unc_col_id_by_name('pod_hash')
        for _id, item in enumerate(self._unc_tasks):
            if item[event_id_id] is not None:
                event_ids.setdefault(item[event_id_id], _id)
            elif item[pod_hash_id] is not None:
                pod_hashes.setdefault(item[pod_hash_id], _id)
        return event_ids, pod_hashes

    @staticmethod
    def _time_delta(item_start: dict, item_end: dict):
        return int(round((item_end.get('end_timestamp') - item_start.get('timestamp')).total_seconds(), 0))

    def _update_uncompleted(self, found_unc_event_ids, found_unc_pod_hashes, found_ids):
        lgng.info(f'UNC_TASKS1={self._unc_tasks}')
        for idx, item in enumerate(self._unc_tasks):
            if (item.get('event_id', None) in found_unc_event_ids) or (
                    item.get('pod_hash', None) in found_unc_pod_hashes):
                del self._unc_tasks[idx]
        lgng.info(f'UNC_TASKS2={self._unc_tasks}')

        unc_data = list()
        for item in self._unc_tasks:
            if item.get('event_id', '') != '':
                unc_data.append(f"id_{item.get('event_id')}")
            elif item.get('pod_hash', '') != '':
                unc_data.append(f"hash_{item.get('pod_hash')}")

        lgng.info(f'found_ids={found_ids}')
        for idx in found_ids:
            if self._found_tasks[idx].get('event_id', '') != '' and f"id_{self._found_tasks[idx].get('event_id')}" not in unc_data:
                self._unc_tasks.append(self._found_tasks[idx])
            elif self._found_tasks[idx].get('pod_hash', '') != '' and f"hash_{self._found_tasks[idx].get('pod_hash')}" not in unc_data:
                self._unc_tasks.append(self._found_tasks[idx])

    '''
    def _add_item_to_uncompleted(self, item):
        self._unc_tasks.append(item)
    '''
    @staticmethod
    def list_to_dict(list_obj: list, list_keys: list):
        dict_obj = dict()
        for idx, list_item in enumerate(list_obj):
            try:
                dict_obj.setdefault(list_keys[idx], list_item)
            except IndexError as e:
                lgng.exception(f'list_to_dict IndexError: list_obj={str(list_obj)}, list_keys={str(list_keys)}, exception={str(e)}')
        return dict_obj

    def load_uncompleted_tasks(self, db_data: list, db_keys: list):
        for db_line in db_data:
            self.add_data(self.list_to_dict(db_line, db_keys))

    def save_completed_uncompleted_tasks(self, data: list, pod_name: str, rule_name: str, rule_id):
        completed_tasks = list()
        completed_tasks_ids = list()
        # rule_data = rules.get(pod_name).get(rule_name)[rule_id]
        task_data = dict()
        for idx in range(len(data)):
            if idx not in completed_tasks_ids:
                cur_item = data[idx]
                if cur_item.get('timestamp', None) is not None:
                    for idx2, item in enumerate(data):
                        if idx2 != idx and idx2 not in completed_tasks_ids:
                            if item.get('end_timestamp', None) is not None and \
                                        item.get('event_id', None) == cur_item.get('event_id', None) and \
                                        item.get('object_type', None) == cur_item.get('object_type', None) and \
                                        item.get('pod_hash', None) == cur_item.get('pod_hash', None):
                                completed_tasks_item = cur_item.copy()
                                completed_tasks_item['end_timestamp'] = item.get('end_timestamp')
                                completed_tasks.append(completed_tasks_item)
                                completed_tasks_ids.append(idx)
                                completed_tasks_ids.append(idx2)

        self._completed_tasks.extend(completed_tasks)

        uncompleted_tasks = list()
        for idx in range(len(data)):
            if idx not in completed_tasks_ids:
                self._uncompleted_tasks.append(data[idx])

    def process_data(self):
        self._uncompleted_tasks = list()
        self._completed_tasks = list()
        for pod_name, rule_data in self._tasks.items():
            for rule_name, rule_id_data in rule_data.items():
                for rule_id in rule_id_data.keys():
                    self.save_completed_uncompleted_tasks(rule_id_data[rule_id], pod_name, rule_name, rule_id)

    def process_data_old(self):
        unc_event_ids, unc_pod_hashes = self._get_uncompleted_event_ids_and_pod_hashes()
        found_unc_event_ids = list()
        found_unc_pod_hashes = list()

        # get ids of pair items and try to find pair to uncompleted events
        prep_data = list()
        found_events = dict()  # format { f'id_{event_id}': [start_id, end_id], f'hash_{pod_hash}': [start_id, end_id]}
        found_ids = list()
        for _id, item in enumerate(self._found_tasks):
            # it this is start_event
            # add start_event to found pool
            if item.get('timestamp', None) is not None:
                if item.get('event_id', '') != '':
                    item_key = f"id_{item.get('event_id')}"
                    if item_key not in found_events.keys():
                        found_events[item_key] = [None, None]
                    found_events[item_key][0] = _id
                    found_ids.append(_id)
                elif item.get('pod_hash', '') != '':
                    item_key = f"hash_{item.get('pod_hash')}"
                    if item_key not in found_events.keys():
                        found_events[item_key] = [None, None]
                    found_events[item_key][0] = _id
                    found_ids.append(_id)
            # if this is end_event
            elif item.get('end_timestamp', None) is not None:
                # check if we have start_event in uncompleted
                if item.get('event_id', '') in unc_event_ids.keys():
                    found_unc_event_ids.append(item.get('event_id'))
                    prep_data_item = self._unc_tasks[unc_event_ids[item.get('event_id')]]
                    prep_data_item['time_delta'] = self._time_delta(prep_data_item, item)
                    prep_data.append(prep_data_item)
                elif item.get('pod_hash', '') in unc_pod_hashes.keys():
                    found_unc_pod_hashes.append(item.get('pod_hash'))
                    prep_data_item = self._unc_tasks[unc_pod_hashes[item.get('pod_hash')]]
                    prep_data_item['time_delta'] = self._time_delta(prep_data_item, item)
                    prep_data.append(prep_data_item)
                # add end_event to found pool
                else:
                    if item.get('event_id', '') != '':
                        item_key = f"id_{item.get('event_id')}"
                        if item_key not in found_events.keys():
                            found_events[item_key] = [None, None]
                        found_events[item_key][1] = _id
                        found_ids.append(_id)
                    elif item.get('pod_hash', '') != '':
                        item_key = f"hash_{item.get('pod_hash')}"
                        if item_key not in found_events.keys():
                            found_events[item_key] = [None, None]
                        found_events[item_key][1] = _id
                        found_ids.append(_id)

        # add pair data to result list, add to uncompleted, if no pair
        for data_id, ids in found_events.items():
            if None in ids:
                lgng.info(f'NO PAIR:data_id={data_id}, ids={str(ids)}')
                '''
                if ids[0]:
                    self._add_item_to_uncompleted(self._found_tasks[ids[0]])
                if ids[1]:
                    self._add_item_to_uncompleted(self._found_tasks[ids[1]])
                '''
            else:
                prep_data_item = self._found_tasks[ids[0]]
                prep_data_item['time_delta'] = self._time_delta(prep_data_item, self._found_tasks[ids[1]])
                prep_data.append(prep_data_item)

        # remove found pair items from uncompleted and add new uncompleted items
        self._update_uncompleted(found_unc_event_ids, found_unc_pod_hashes, found_ids)
        # clear found tasks list
        self._found_tasks = list()

        self._prepared_data = prep_data
        return True

    def get_found_tasks(self):
        return self._completed_tasks

    def get_unc_tasks(self):
        return self._uncompleted_tasks

    def dump_tasks_to_file(self, file_path):
        with open(file_path, "wb") as file:
            pickle.dump(self._tasks, file)

    def dump_found_tasks_to_file(self):
        path = '/tmp/tasks.dump'
        with open(path, 'w', encoding='utf-8') as f:
            f.write(str(len(self._found_tasks)))
            for item in self._found_tasks:
                f.write(json.dumps(item, ensure_ascii=False, default=str))
                f.write('\n')
        return path
