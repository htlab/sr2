package soxrecorderv2.recorder;

import java.util.ArrayList;
import java.util.List;

/**
 * runs as a daemon, subscribe sox nodes with a single connection,
 * and just waiting for data arrival, save data to PostgreSQL directly when data is received.
 * 
 * (recording process skips calling Controlller API to reduce overhead and gain some performance advantage.)
 * 
 * @author tomotaka
 *
 */
public class Recorder {
	
	private DBWriterProcess writer;
	private List<ServerRecordProcess> serverRecorders = new ArrayList<>();
	
	public static void main(String[] args) {
		
	}

}
