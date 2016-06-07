package soxrecorderv2.exporter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.io.IOUtils;


/**
 * telnet based simple TCP command server
 * 
 * @author tomotaka
 *
 */
public class ExporterServer implements Runnable {
	
	private class ClientHandler implements Runnable {
		private final Socket client;
		
		public ClientHandler(Socket client) {
			this.client = client;
		}
		
		@Override
		public void run() {
			final Exporter system = ExporterServer.this.getSystem();
			InputStreamReader inReader = null;
			try {
				inReader = new InputStreamReader(client.getInputStream());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (inReader == null) {
				return;
			}
			BufferedReader reader = new BufferedReader(inReader);
			while (true) {
				String line = null;
				try {
					line = reader.readLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if (line == null) {
					break;
				}
				
				if (line.toLowerCase().equals("exit")) {
					break;
				}
				
				if (line.startsWith("start ")) {
					String[] chunks = line.split(" ");
					long exportId = Long.parseLong(chunks[1]);
					
					system.enqueueExportTask(exportId);
				}
			}
			
			IOUtils.closeQuietly(client);
		}
	}
	
	private final Exporter system;
	
	public ExporterServer(Exporter system) {
		this.system = system;
	}
	
	public Exporter getSystem() {
		return system;
	}

	@SuppressWarnings("resource")
	@Override
	public void run() {
		final int port = 1111; // TODO: lookup port number from config
		ServerSocket server = null;
		try {
			server = new ServerSocket(port);
		} catch (IOException e1) {
			// TODO: logging
			e1.printStackTrace();
		}
		
		while (true) {
			try {
				final Socket client = server.accept();
				Thread clientHandler = new Thread(new ClientHandler(client));
				clientHandler.start();
			} catch (IOException e) {
				// TODO: logging
				e.printStackTrace();
			}
		}
	}

}
