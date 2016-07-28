CREATE TABLE "user"(
	id bigserial,
	login varchar(255) NOT NULL,
	hash_pw varchar(255) NOT NULL,
	hash_seed varchar(255) NOT NULL,
	created timestamp NOT NULL,
	PRIMARY KEY (id),
	UNIQUE (login)
);

CREATE TABLE "api_key"(
	user_id bigint REFERENCES "user" (id),
	is_enabled boolean NOT NULL,
	api_key varchar(255) NOT NULL,
	created timestamp NOT NULL,
	PRIMARY KEY (api_key)
);

CREATE TABLE observation(
	id bigserial,
	sox_server varchar(255) NOT NULL,
	sox_node varchar(255) NOT NULL,
	sox_jid varchar(255),
	sox_password varchar(255),
	is_using_default_user boolean NOT NULL,
	is_existing boolean NOT NULL,
	is_record_stopped boolean NOT NULL,
	is_subscribed boolean NOT NULL,
	created timestamp NOT NULL,
	is_subscribe_failed boolean,
	recent_monthly_average_data_arrival real,
	recent_monthly_total_data_arrival integer,
	recent_monthly_data_available_days integer,
	PRIMARY KEY (id),
	UNIQUE (sox_server, sox_node)
);

CREATE INDEX ob_rmada ON observation (recent_monthly_average_data_arrival);
CREATE INDEX ob_rmtda ON observation (recent_monthly_total_data_arrival);
CREATE INDEX ob_rmdad ON observation (recent_monthly_data_available_days);
CREATE INDEX ob_created ON observation (created);

CREATE TABLE daily_record_count(
	id bigserial,
    observation_id bigint REFERENCES observation (id),
	day date NOT NULL,
	daily_total_count bigint NOT NULL,
    PRIMARY KEY (id),
	UNIQUE (observation_id, day)
);

CREATE TABLE daily_unit(
	daily_record_count_id bigint REFERENCES daily_record_count (id),
	unit smallint NOT NULL,
	unit_seq smallint NOT NULL,
	count bigint NOT NULL,
	PRIMARY KEY (daily_record_count_id, unit, unit_seq)
);

CREATE TABLE monthly_record_count(
	id bigserial,
	observation_id bigint REFERENCES observation (id),
	year smallint NOT NULL,
	month smallint NOT NULL,
	monthly_total_count bigint NOT NULL,
	PRIMARY KEY (id),
	UNIQUE (observation_id, year, month)
);

CREATE TABLE monthly_unit(
	monthly_record_count_id bigint REFERENCES monthly_record_count (id),
	day smallint NOT NULL,
	day_count bigint NOT NULL,
	PRIMARY KEY (monthly_record_count_id, day)
);

CREATE TABLE record(
	id bigserial,
	observation_id bigint REFERENCES observation (id),
	is_parse_error boolean NOT NULL,
	created timestamp NOT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE raw_xml(
	id bigserial,
	record_id bigint REFERENCES record (id),
    is_gzipped boolean NOT NULL,
	raw_xml bytea NOT NULL,
	PRIMARY KEY (id)
);

CREATE INDEX record_observation_id_created ON record (observation_id, created);

/*
	hash_key => SHA-256 hash hex digest
*/
CREATE TABLE large_object(
	id bigserial,
	is_gzipped boolean,
	hash_key varchar(64) NOT NULL,
	content bytea NOT NULL,
	content_length integer NOT NULL,
	PRIMARY KEY (id),
	UNIQUE (hash_key)
);


/*
	name => "current temperature"
	transdcuer_id  => "temp"
*/
CREATE TABLE transducer(
	id bigserial,
	observation_id bigint REFERENCES observation (id),
	name varchar(255),
	transducer_id varchar(255),
	min_value varchar(255),
	max_value varchar(255),
	unit varchar(255),
	PRIMARY KEY (id),
	UNIQUE (observation_id, transducer_id)
);

/*
	value_type:
		1: string
		2: int
		3: float
		4: decimal
		5: large object
*/
CREATE TABLE transducer_raw_value(
	record_id bigint REFERENCES record (id),
	has_same_typed_value boolean NOT NULL,
    value_type smallint NOT NULL,
	transducer_id bigint REFERENCES transducer (id),
	string_value varchar(255),
	int_value bigint,
	float_value double precision,
	decimal_value decimal,
	large_object_id bigint REFERENCES large_object (id),
	transducer_timestamp timestamp,
	PRIMARY KEY (record_id, transducer_id)
);

CREATE TABLE transducer_typed_value(
	record_id bigint REFERENCES record (id),
	value_type smallint NOT NULL,
	transducer_id bigint REFERENCES transducer (id),
	string_value varchar(255),
	int_value bigint,
	decimal_value decimal,
	float_value double precision,
	large_object_id bigint REFERENCES large_object (id),
	PRIMARY KEY (record_id, transducer_id)
);

CREATE TABLE export(
    id bigserial,
	observation_id bigint REFERENCES observation (id),
    time_start timestamp NOT NULL,
    time_end timestamp NOT NULL,
    format varchar(255) NOT NULL,
    is_gzipped boolean NOT NULL,
    is_include_xml boolean NOT NULL,
    file_name text NOT NULL,
    save_until timestamp NOT NULL,
    created timestamp NOT NULL,
    state smallint NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE export_transducer(
	export_id bigint REFERENCES export (id),
	transducer_id bigint REFERENCES transducer (id),
	seq int NOT NULL,
	PRIMARY KEY (export_id, transducer_id)
);

CREATE INDEX export_created ON export (created);
CREATE INDEX export_state_save_until ON export (state, save_until);
CREATE INDEX export_state_created ON export (state, created);

/*
    state:
        1: waiting for start (INITIATED)
		2: in queue (IN_QUEUE)
        3: exporting (STARTED)
        4: export finished (FINISHED)
        5: expired(removed) (EXPIRED)
		-1: ERROR
*/

CREATE TABLE event_log(
	id bigserial,
	system varchar(255),
	component varchar(255),
	sox_server varchar(255),
	sox_node varchar(255),
	message varchar(255),
	log_level integer,
	log_type integer,
	logged_at timestamp,
	has_stacktrace boolean,
	uuid varchar(255),
	PRIMARY KEY (id),
	UNIQUE (uuid)
);
CREATE INDEX event_log_idx ON event_log (logged_at, log_level, system, component, log_type);

CREATE TABLE event_log_stacktrace(
	id bigserial,
	event_log_uuid varchar(255) REFERENCES event_log (uuid),
	stacktrace text,
	PRIMARY KEY (id)
);
CREATE INDEX event_log_stacktrace_uuid ON event_log_stacktrace (event_log_uuid);

CREATE TABLE blacklist(
	sox_server varchar(255) NOT NULL,
	sox_node varchar(255) NOT NULL,
	memo varchar(255),
	PRIMARY KEY (sox_server, sox_node)
);
