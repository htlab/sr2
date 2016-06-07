# -*- coding: utf-8 -*-
import json

from sr2 import models


class BaseBatchEngine(object):
    def __init__(self, config_file):
        self._configure(config_file)

    def _configure(self, config_file):
        self._config_file = config_file
        with open(config_file, 'rb') as fh:
            self._config = json.load(fh)

        self._connect_postgresql(self._config['postgresql'])

    def _connect_postgresql(self, pg_info):
        models.connect(
            pg_info.get('host', 'localhost'),
            pg_info.get('port', 5432),
            pg_info.get('user', 'sr2'),
            pg_info.get('pw', 'sr2miro'),
            pg_info.get('db', 'sr2')
        )

    def run(self):
        raise NotImplementedError()
