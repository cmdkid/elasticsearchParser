rules_neurodata_api = {
    'time_between_events': [
        {
            'event_id_regex': 'sid: ([a-z0-9]{16})',
            'start_event': 'Start processing',
            'end_event': 'Task finished'
        }
    ]
}

rules_neurodata_unit = {
    'time_between_events': [
        {
            'event_id_regex': None,
            'start_event': 'Initializing Processing Node. PPID',
            'end_event': 'Processing loop PID'
        }
    ]
}

rules = {
    'neurodata-api': rules_neurodata_api,
    'neurodata-unit-bp': rules_neurodata_unit,
    'neurodata-unit-brt': rules_neurodata_unit,
    'neurodata-unit-fan': rules_neurodata_unit,
    'neurodata-unit-fd-codecoverage': rules_neurodata_unit,
    'neurodata-unit-fd-debug': rules_neurodata_unit,
    'neurodata-unit-fd': rules_neurodata_unit,
    'neurodata-unit-hr-debug': rules_neurodata_unit,
    'neurodata-unit-hr': rules_neurodata_unit,
    'neurodata-unit-mm-debug': rules_neurodata_unit,
    'neurodata-unit-mm': rules_neurodata_unit,
    'neurodata-unit-oed': rules_neurodata_unit,
    'neurodata-unit-pd': rules_neurodata_unit,
    'neurodata-unit-si': rules_neurodata_unit,
}

rules_pods_list = rules.keys()

'''
    Вывести графики по количеству установленных GRPC соединений
    Вывести графики по временам обработки запросов
    Вывести графики по временам инициализации воркеров
    Вывести графики по памяти утилизированным кубернетесом/воркерами
'''