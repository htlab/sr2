# -*- coding: utf-8 -*-
import sys
import getpass

import click

from sr2.models import (
    USER_LEVEL_ADMIN,
    connect,
    User
)


@click.command()
@click.option('-h', '--pg-host', help='postgresql host (default: localhost)', default='localhost')
@click.option('-u', '--pg-user', help='postgresql user (default: sr2)', default='sr2')
@click.option('-d', '--pg-database', help='postgresql db (default: sr2)', default='sr2')
@click.option('-p', '--pg-port', type=int, help='postgresql port (default: 5432)', default=5432)
@click.option('-a', '--admin-name', help='admin user name (default: admin)', default='admin')
def main(pg_host, pg_user, pg_database, pg_port, admin_name):
    # pw_input = raw_input('postgresql password: ')
    print 'PostgreSQL host = %s' % pg_host
    pw_input = getpass.getpass('postgresql password: ')
    if not pw_input:
        print 'no password given'
        sys.exit(1)

    connect(pg_host, pg_port, pg_user, pw_input, pg_database)

    print 'going to add new admin user \'%s\'' % admin_name

    chk_user = User.select().where(User.login == admin_name).first()
    if chk_user is not None:
        print 'Abort: user \'%s\' is existing!' % admin_name
        sys.exit(1)

    # sr2_user_pw_input = raw_input('password for user \'%s\':')
    sr2_user_pw_input = getpass.getpass('password for user \'%s\':' % admin_name)
    while not sr2_user_pw_input or len(sr2_user_pw_input) < 5:
        print 'please input 5 chars or longer password'
        sr2_user_pw_input = getpass.getpass('password for user \'%s\':' % admin_name)

    sr2_user_pw_input_c = getpass.getpass('password for user \'%s\'(confirm):' % admin_name)
    while sr2_user_pw_input_c != sr2_user_pw_input:
        print 'confirmation failed: please input same password'
        sr2_user_pw_input_c = getpass.getpass('password for user \'%s\'(confirm):' % admin_name)

    new_admin_user = User(login=admin_name)
    new_admin_user.level = USER_LEVEL_ADMIN
    new_admin_user.assign_password(sr2_user_pw_input)
    new_admin_user.save()

    print 'created new user: %s' % admin_name


if __name__ == '__main__':
    main()
