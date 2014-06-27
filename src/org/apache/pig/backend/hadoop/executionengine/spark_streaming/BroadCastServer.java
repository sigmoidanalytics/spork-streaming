package org.apache.pig.backend.hadoop.executionengine.spark_streaming;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

/* Relay server thread to serve missed Objects */

public class BroadCastServer extends Thread{

	private ServerSocket serverSocket;
	private Map<Object, Object> storage = null;

	private String test = null;
	int port;

	public BroadCastServer(){
		
	}
	
	public BroadCastServer(int port,String test) throws IOException{
		
		this.port = port;		
		this.test = test;
		
		serverSocket = new ServerSocket(port);
		//serverSocket.setSoTimeout(10000);

		
	}

	public void run(){
		while(true){
			try{
				
				System.out.println("Waiting for client on port " +
						serverSocket.getLocalPort() + "...");

				Socket server = serverSocket.accept();
				System.out.println("Just connected to "
						+ server.getRemoteSocketAddress());
				DataInputStream in =
						new DataInputStream(server.getInputStream());
				System.out.println("Executor asking for :" + in.readUTF());
				DataOutputStream out =
						new DataOutputStream(server.getOutputStream());
				
				out.writeUTF(test);
				
				server.close();

			}catch(Exception s){				
				s.printStackTrace();
				break;
			}
		}
	}


	public void startBroadcastServer(int port, String test){

		try{

			Thread t = new BroadCastServer(port, test);
			t.start();

		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
