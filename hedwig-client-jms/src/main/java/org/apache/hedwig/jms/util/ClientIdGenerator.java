package org.apache.hedwig.jms.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

public class ClientIdGenerator {

	static long counter=0;
	
	public static String getNewClientId() {
		try {
	        return "CLIENT_ID/"+ counter++ +"/"+InetAddress.getLocalHost().getHostName()+"@"+System.currentTimeMillis();
        } catch (UnknownHostException e) {
	        Logger.getLogger(ClientIdGenerator.class).error("Cannot resolve local host name", e);
        } 
		return null;
	}
}
