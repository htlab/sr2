package soxrecorderv2.logging;

public enum SR2LogType {
	
	SUBSCRIBE_FAILED           ( 20000 ),
	SUBSCRIBE                  ( 10000 ),
	RECORD_FAILED              ( 20001 ),
	RECORD                     ( 10001 ),
	TRANSDUCER_CREATE_FAILED   ( 20002 ),
	TRANSDUCER_CREATE          ( 10002 ),
	LARGE_OBJECT_CREATE_FAILED ( 20003 ),
	LARGE_OBJECT_CREATE        ( 10003 ),
	OBSERVATION_CREATE_FAILED  ( 20004 ),
	OBSERVATION_CREATE         ( 10004 ),
	PG_CONNECT_FAILED          ( 20005 ),
	PG_CONNECT                 ( 10005 ),
	CACHE_CONNECT_FAILED       ( 20006 ),
	CACHE_CONNECT              ( 10006 ),
	
	FINDER_START               ( 10007 ),
	FINDER_STOP                ( 10008 ),
	
	DB_WRITER_START            ( 10009 ),
	DB_WRITER_STOP             ( 10010 ),
	
	RECORD_PROCESS_START       ( 10011 ),
	RECORD_PROCESS_STOP        ( 10012 ),
	
	STATE_SYNCHRONIZER_START   ( 10013 ),
	STATE_SYNCHRONIZER_STOP    ( 10014 ),
	
	SYSTEM_START               ( 10015 ),
	SYSTEM_STOP                ( 10016 ),
	
	RAW_XML_INSERT_FAILED      ( 20017 ),
	RAW_XML_INSERT             ( 10017 ),
	
	RAW_VALUE_INSERT_FAILED    ( 20018 ),
	RAW_VALUE_INSERT           ( 10018 ),
	
	TYPED_VALUE_INSERT_FAILED  ( 20019 ),
	TYPED_VALUE_INSERT         ( 10019 ),
	
	JAVA_INTERRUPTED_EXCEPTION ( 20020 ),
	JAVA_GENERAL_EXCEPTION     ( 20021 ),
	JAVA_SQL_EXCEPTION         ( 20022 ),
	JAVA_XPATH_EXCEPTION       ( 20023 ),
	
	RECORD_GIVE_UP             ( 20024 ),
	
	UNSUBSCRIBE_FAILED         ( 20025 ),
	
	SOX_CONN_RENEW             ( 10026 )
	
	;
	
	private int number;
	
	SR2LogType(int number) {
		this.number = number;
	}
	
	public int getNumber() {
		return number;
	}

}
