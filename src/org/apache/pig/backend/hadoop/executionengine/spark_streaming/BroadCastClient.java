package org.apache.pig.backend.hadoop.executionengine.spark_streaming;

import java.net.Socket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class BroadCastClient {

	private String host;
	private int port;
	
	public BroadCastClient(String host, int port){

		this.host = host;
		this.port = port;

	}

	public String getBroadCastMessage(String request){

		String response = "";
		
		try{
			
			System.out.println("Connecting to " + host
					+ " on port " + port);
			Socket client = new Socket(host, port);
			
			OutputStream outToServer = client.getOutputStream();
			DataOutputStream out = new DataOutputStream(outToServer);

			out.writeUTF(request);
			InputStream inFromServer = client.getInputStream();
			DataInputStream in = new DataInputStream(inFromServer);	
			response = in.readUTF();
			System.out.println("Server says " + response);
			client.close();
			
			
		}catch(Exception e){
			e.printStackTrace();
		}

		return response;
	}
}
