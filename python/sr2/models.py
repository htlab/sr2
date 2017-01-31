# -*- coding: utf-8 -*-
import hashlib
import datetime
import os

from peewee import (
    SQL,
    Model,
    PostgresqlDatabase,
    Proxy,
    CompositeKey,
    PrimaryKeyField,
    BooleanField,
    IntegerField,
    BigIntegerField,
    CharField,
    TextField,
    DateField,
    DateTimeField,
    BlobField,
    FloatField,
    DoubleField,
    ForeignKeyField
)

from sr2.util import gen_random_string

USER_LEVEL_NORMAL = 0
USER_LEVEL_ADMIN = 10

LIST_TYPE_WHITE = 'white'
LIST_TYPE_BLACK = 'black'


database_proxy = Proxy()


def connect(host, port, user, pw, db):
    pgdb = PostgresqlDatabase(
        database=db,
        user=user,
        password=pw,
        host=host,
        port=port
    )
    database_proxy.initialize(pgdb)


class BaseModel(Model):
    class Meta:
        database = database_proxy


class User(BaseModel):
    id = PrimaryKeyField(null=False, primary_key=True)
    login = CharField(null=False, unique=True)
    level = IntegerField(
        null=False,
        choices=(USER_LEVEL_NORMAL, USER_LEVEL_NORMAL),
        default=USER_LEVEL_NORMAL
    )
    hash_pw = CharField(null=False)
    hash_seed = CharField(null=False)
    created = DateTimeField(null=False, default=datetime.datetime.now)

    def assign_password(self, raw_pw):
        seed = gen_random_string(255)
        hashed = self._gen_hash(seed, raw_pw)
        self.hash_pw = hashed
        self.hash_seed = seed

    def auth(self, raw_pw):
        print 'correct  =' + self.hash_pw
        print 'generated=' + self._gen_hash(self.hash_seed, raw_pw)
        print 'generated=' + self._gen_hash(self.hash_seed, raw_pw)
        return (self.hash_pw == self._gen_hash(self.hash_seed, raw_pw))

    def _gen_hash(self, seed, raw_pw):
        strech_times = 100
        hashed = seed + raw_pw
        for _ in xrange(strech_times):
            hashed = hashlib.sha256(hashed).hexdigest()
        return hashed

    def is_admin(self):
        return self.level == USER_LEVEL_ADMIN


class ApiKey(BaseModel):
    # user_id = BigIntegerField(null=False)
    user = ForeignKeyField(User, db_column='user_id', related_name='api_keys')
    is_enabled = BooleanField(null=False)
    api_key = CharField(null=False, primary_key=True)
    created = DateTimeField(null=False, default=datetime.datetime.now)

    def assign_random_key(self):
        self.api_key = hashlib.md5(os.urandom(100)).hexdigest()

    class Meta:
        database = database_proxy
        db_table = 'api_key'
        constraints = (
            SQL('FOREIGN KEY (user_id) REFERENCES "user" (id)'),
        )


class NodeList(BaseModel):
    id = PrimaryKeyField(null=False, primary_key=True)
    name = CharField(null=False, unique=True)
    created = DateTimeField(null=False, default=datetime.datetime.now)
    means_all = BooleanField(null=False, default=False)

    class Meta:
        database = database_proxy
        indexes = (
            (('name',), True)
        )


class NodeListMember(BaseModel):
    id = PrimaryKeyField(null=False, primary_key=True)
    node_list_id = BigIntegerField(null=False)
    observation_id = BigIntegerField(null=False)
    created = DateTimeField(null=False, default=datetime.datetime.now)

    class Meta:
        database = database_proxy
        constraints = (
            SQL('FOREIGN KEY (node_list_id) REFERENCES node_list (id)'),
            SQL('FOREIGN KEY (observation_id) REFERENCES observation (id)'),
        )
        indexes = (
            (('node_list_id',), False)
        )


class UserAccessCtrl(BaseModel):
    id = PrimaryKeyField(null=False, primary_key=True)
    name = CharField(null=False, unique=True)
    user_id = BigIntegerField(null=False)
    node_list_id = BigIntegerField(null=False)
    list_type = CharField(
        null=False,
        choices=(LIST_TYPE_WHITE, LIST_TYPE_BLACK)
    )
    created = DateTimeField(null=False, default=datetime.datetime.now)

    class Meta:
        database = database_proxy
        constraints = (
            SQL('FOREIGN KEY (user_id) REFERENCES "user" (id)'),
            SQL('FOREIGN KEY (node_list_id) REFERENCES node_list (id)'),
        )
        indexes = (
            (('name',), False),
            (('user_id',), False)
        )


class Observation(BaseModel):
    id = PrimaryKeyField(null=False, primary_key=True)
    sox_server = CharField(null=False)
    sox_node = CharField(null=False)
    sox_jid = CharField(null=False)
    sox_password = CharField(null=False)
    is_anonymous = BooleanField(null=False, default=True)
    is_existing = BooleanField(null=False, default=True)
    is_record_stopped = BooleanField(null=False, default=False)
    recent_monthly_average_data_arrival = FloatField(default=-1.0)
    recent_monthly_total_data_arrival = IntegerField(default=-1)
    recent_monthly_data_available_days = IntegerField(default=-1)
    created = DateTimeField(null=False, default=datetime.datetime.now)

    class Meta:
        database = database_proxy
        indexes = (
            (('sox_server', 'sox_node'), True),
            (('recent_monthly_average_data_arrival',), False),
            (('recent_monthly_total_data_arrival',), False),
            (('recent_monthly_data_available_days',), False),
            (('created',), False)
        )


class DailyRecordCount(BaseModel):
    id = PrimaryKeyField(null=False, primary_key=True)
    observation_id = BigIntegerField(null=False)
    day = DateField(null=False)
    daily_total_count = BigIntegerField(null=False)

    class Meta:
        database = database_proxy
        constraints = (
            SQL('FOREIGN KEY (observation_id) REFERENCES observation (id)'),
        )
        indexes = (
            (('observation_id', 'day'), True)
        )


class DailyUnit(BaseModel):
    daily_record_count_id = BigIntegerField(null=False)
    unit = IntegerField(null=False)
    unit_seq = IntegerField(null=False)
    count = BigIntegerField(null=False)

    class Meta:
        database = database_proxy
        primary_key = CompositeKey('daily_record_count_id', 'unit', 'unit_seq')
        constraints = (
            SQL('FOREIGN KEY (daily_record_count_id) REFERENCES daily_record_count (id)'),
        )


class MonthlyRecordCount(BaseModel):
    id = PrimaryKeyField(null=False, primary_key=True)
    observation_id = BigIntegerField(null=False)
    year = IntegerField(null=False)
    month = IntegerField(null=False)
    monthly_total_count = BigIntegerField(null=False)

    class Meta:
        database = database_proxy
        constraints = (
            SQL('FOREIGN KEY (observation_id) REFERENCES observation (id)'),
        )
        indexes = (
            (('observation_id', 'year', 'month'), True),
        )


class MonthlyUnit(BaseModel):
    monthly_record_count_id = BigIntegerField(null=False)
    day = IntegerField(null=False)
    day_count = BigIntegerField(null=False)

    class Meta:
        database = database_proxy
        primary_key = CompositeKey('monthly_record_count_id', 'day')


class RawXml(BaseModel):
    id = PrimaryKeyField(null=False, primary_key=True)
    is_gzipped = BooleanField(null=False)
    raw_xml = BlobField(null=False)


class Record(BaseModel):
    id = PrimaryKeyField(null=False, primary_key=True)
    observation_id = BigIntegerField(null=False)
    is_parse_error = BooleanField(null=False)
    raw_xml_id = BigIntegerField(null=False)
    created = DateTimeField(null=False)

    class Meta:
        database = database_proxy
        constraints = (
            SQL('FOREIGN KEY (observation_id) REFERENCES observation (id)'),
            SQL('FOREIGN KEY (raw_xml_id) REFERENCES raw_xml (id)')
        )
        indexes = (
            (('observation_id', 'created'), False),
        )


class LargeObject(BaseModel):
    id = PrimaryKeyField(null=False, primary_key=True)
    is_gzipped = BooleanField(null=False)
    hash_key = CharField(null=False)
    content = BlobField(null=False)

    class Meta:
        database = database_proxy
        indexes = (
            (('hash_key',), True),
        )


class TransducerRawValue(BaseModel):
    record_id = BigIntegerField(null=False)
    has_same_typed_value = BooleanField(null=False)
    value_type = IntegerField(null=False)
    transducer = CharField(null=False)
    string_value = CharField()
    int_value = BigIntegerField()
    float_value = DoubleField()
    large_object_id = BigIntegerField()
    transducer_timestamp = DateTimeField()

    class Meta:
        database = database_proxy
        primary_key = CompositeKey('record_id', 'transducer')
        constraints = (
            SQL('FOREIGN KEY large_object_id REFERENCES large_object (id)'),
        )


class TransducerTypedValue(BaseModel):
    record_id = BigIntegerField(null=False)
    value_type = IntegerField(null=False)
    transducer = CharField(null=False)
    string_value = CharField()
    int_value = BigIntegerField()
    float_value = DoubleField()
    large_object_id = BigIntegerField()

    class Meta:
        database = database_proxy
        primary_key = CompositeKey('record_id', 'transducer')
        constraints = (
            SQL('FOREIGN KEY large_object_id REFERENCES large_object (id)'),
        )


class Export(BaseModel):
    id = PrimaryKeyField(null=False, primary_key=True)
    observation_id = BigIntegerField(null=False)
    time_start = DateTimeField(null=False)
    time_end = DateTimeField(null=False)
    format = CharField(null=False)
    is_gzipped = BooleanField(null=False)
    is_include_xml = BooleanField(null=False)
    file_name = TextField(null=False)
    save_until = DateTimeField(null=False)
    created = DateTimeField(null=False)
    state = IntegerField(null=False)
    is_failed = BooleanField(null=False)

    class Meta:
        database = database_proxy
        constraints = (
            SQL('FOREIGN KEY (observation_id) REFERENCES observation (id)'),
        )
        indexes = (
            (('created',), False),
            (('state', 'save_until'), False),
            (('state', 'created'), False)
        )
