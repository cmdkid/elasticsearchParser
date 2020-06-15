from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
from datetime import datetime, timezone
'''
CRITICAL = 50
ERROR = 40
WARNING = 30
INFO = 20
DEBUG = 10
NOTSET = 0
'''

conf = {
    'elastic_server': 'http://elasticsearch:9200',
    'db': {
        # ssh -f -L 5432:35.223.76.15:5432 -N y.medovikov@62.140.237.114 -p 8867
        'host': 'localhost',  # '35.223.76.15', 'localhost',
        'port': 5432,
        'db_name': 'es_parser',
        'user': 'postgres',  # 'cul_api',
        'password': 'ndlKuberCul57#dev'
    },
    'es': {
        'index_prefix': 'logstash-'
    },
    'log': {
        'level': INFO,
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    },
    'dumper': {
        # dump mode ['dump_tasks', 'dump_db_data'] or normal run (False).
        # stages: es_dump (by dump_elastic_data.py script in pod), then dump_tasks, then dump_db_data
        # get es_dump:
        # run k8s pod with by dump_elastic_data.py script
        # watch -n 10 -d 'kubectl logs -n kube-logging es-parser-dump-67cd95b7f7-wcm4t | grep -e "items complete"'
        # kubectl cp kube-logging/es-parser-dump-67cd95b7f7-wcm4t:es.tar.gz ./ed_dump.tar.gz
        # scp nlab@192.168.57.90:/home/nlab/kubeconfigs/logging/ed_dump.tar.gz .
        'dump_mode': 'dump_db_data',
        'dump_use': True,  # use dump or real elastic data
        'dump_date': datetime(2020, 6, 14, tzinfo=timezone.utc),  # datetime(2020, 4, 18, tzinfo=timezone.utc) datetime(2020, 4, 27, tzinfo=timezone.utc)
        'dump_path': '/Users/dsdr/PycharmProjects/dumps/dump_1592200890',  # '/Users/dsdr/PycharmProjects/dumps/dump_1587733103',
        'tasks_dump_path': '/Users/dsdr/PycharmProjects/dumps/tasks_dump_1592200890.dat',  # '/Users/dsdr/PycharmProjects/dumps/tasks_dump_1587733103.dat',
    }
}