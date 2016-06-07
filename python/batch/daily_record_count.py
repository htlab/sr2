# -*- coding: utf-8 -*-
import os
import os.path
import sys

import click

from sr2.batch.daily_record_count_engine import DailyRecordCountEngine


@click.command()
@click.option('-c', '--config-file')
def main(config_file):
    if not os.path.exists(config_file):
        print 'missing config file: %s' % config_file
        sys.exit(-1)

    batch = DailyRecordCountEngine(config_file)
    batch.run()
