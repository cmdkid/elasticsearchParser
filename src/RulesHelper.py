from rules import rules, rules_pods_list


class RulesHelper:
    trigger_types = {
        'start_event',
        'end_event'
    }


    @staticmethod
    def is_pod_in_rules(pod_name):
        if pod_name in rules_pods_list:
            return True
        else:
            return False

    @staticmethod
    def is_triggered_log(pod_name, log):
        for rule_type, rule_list in rules.get(pod_name).items():
            if 'time_between_events' == rule_type:
                for rule_id, rule in enumerate(rule_list):
                    if rule.get('start_event') in log:
                        return rule_type, rule_id, 'start_event'
                    if rule.get('end_event') in log:
                        return rule_type, rule_id, 'end_event'
        return None, None, None

    @staticmethod
    def parse_rule(rule_name, trigger_type, hit_obj):
        if 'time_between_events' == rule_name:
            pod_name, pod_hash = hit_obj.get_pod_info()
            for rule_item in rules.get(pod_name).get(rule_name):
                event_id_regex = rule_item.get('event_id_regex')
                timestamp = hit_obj.get_timestamp(pod_name)
                data = {
                    'pod_name': pod_name,
                    'pod_hash': pod_hash,
                    'rule_type': rule_name,
                }
                event_id = hit_obj.get_event_id(event_id_regex)
                if event_id is not None:
                    data.setdefault('event_id', event_id)
                object_type = hit_obj.get_object_type(pod_name)
                if object_type is not None:
                    data.setdefault('object_type', object_type)

                if 'start_event' == trigger_type:
                    data['timestamp'] = timestamp
                    return data
                elif 'end_event' == trigger_type:
                    data['end_timestamp'] = timestamp
                    return data
        return None

