package com.bah.na.asc.storage.record.management.elasticsearch;

import static org.junit.Assert.assertTrue;
import java.net.UnknownHostException;
import java.util.List;

import org.elasticsearch.client.Client;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.bah.na.asc.core.system.EnvProperty;
import com.bah.na.asc.storage.core.elasticsearch.ElasticsearchClient;

public class IdentifyNearExpiredRecordsTest{
	protected static final String esIp = EnvProperty.getInstance().getEnvVar("ASC_ES_IP");
	protected static final String esPort = EnvProperty.getInstance().getEnvVar("ASC_ES_PORT");
	protected static final String esTransportPort = EnvProperty.getInstance().getEnvVar("ASC_ES_TRANSPORT_PORT");
	protected static final String esClusterName = EnvProperty.getInstance().getEnvVar("ASC_ES_CLUSTER_NAME");
	protected static final String TEST_TYPE = "tp";
	protected static Client client = null;
	protected static IdentifyNearExpiredRecords identifyRecordsConnection = null;
	static String date0;
	static String date1;
	static String date2;

	/**
	 * Ingest
	 * 
	 * @throws Exception
	 */
	@Ignore
	@BeforeClass
	public static void setUpandIngest() throws Exception{
		try{
			client = ElasticsearchClient.getElasticsearchClient(esClusterName, esIp, Integer.parseInt(esPort));
			identifyRecordsConnection = new IdentifyNearExpiredRecords(client);
		}catch(UnknownHostException uhe){
			System.out.println("Unknown Host!");
		}catch(Exception e){
			e.printStackTrace();
		}
		date0 = identifyRecordsConnection.calculateFutureDate(0);
		date1 = identifyRecordsConnection.calculateFutureDate(1);
		date2 = identifyRecordsConnection.calculateFutureDate(2);
		try{
			client.prepareIndex("1234567890-dogwood@bah.com-" + date0, TEST_TYPE, "1").setSource().get();
			client.prepareIndex("1234567891-birch@bah.com-" + date1, TEST_TYPE, "1").setSource().get();
			client.prepareIndex("1234567892-aspen@bah.com-" + date2, TEST_TYPE, "1").setSource().get();
			Thread.sleep(4000);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	/**
	 * testing getExpiringRecords() method
	 * 
	 * @throws Exception
	 */
	@Ignore
	@Test
	public void testGetExpiringRecords() throws Exception{
		List<String> recordsList = identifyRecordsConnection.getExpiringRecords(2);
		assertTrue(recordsList.contains("1234567892-aspen@bah.com-" + date2));
	}

	@Ignore
	@AfterClass
	public static void afterClass() throws Exception{
		client.close();
	}
}
