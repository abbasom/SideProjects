package com.bah.na.asc.storage.record.management.elasticsearch;

import static org.junit.Assert.assertEquals;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.bah.na.asc.core.system.EnvProperty;
import com.bah.na.asc.storage.core.elasticsearch.ElasticsearchClient;

public class PostNearExpiredRecordsTest{
	protected static final String DOCUMENT_INDEX = "about_to_expire";
	protected static final String esIp = EnvProperty.getInstance().getEnvVar("ASC_ES_IP");
	protected static final String esPort = EnvProperty.getInstance().getEnvVar("ASC_ES_PORT");
	protected static final String esTransportPort = EnvProperty.getInstance().getEnvVar("ASC_ES_TRANSPORT_PORT");
	protected static final String esClusterName = EnvProperty.getInstance().getEnvVar("ASC_ES_CLUSTER_NAME");
	protected static final String TEST_TYPE = "tp";
	protected static Client client = null;
	protected static PostNearExpiredRecords postRecordsConnection = null;
	protected static IdentifyNearExpiredRecords identifyRecordsConnection = null;
	static String date0;
	static String date1;
	static String date2;

	@Ignore
	@BeforeClass
	public static void setUpandIngest() throws Exception{
		try{
			client = ElasticsearchClient.getElasticsearchClient(esClusterName, esIp, Integer.parseInt(esPort));
			postRecordsConnection = new PostNearExpiredRecords(client);
			identifyRecordsConnection = new IdentifyNearExpiredRecords(client);
		}catch(UnknownHostException uhe){
			System.out.println("Unknown Host!");
		}catch(Exception e){
			e.printStackTrace();
		}
		date0 = identifyRecordsConnection.calculateFutureDate(0);
		date1 = identifyRecordsConnection.calculateFutureDate(1);
		date2 = identifyRecordsConnection.calculateFutureDate(2);
		List<String> recordNames = new ArrayList<String>();
		recordNames.add("1234567890-dogwood@bah.com-" + date0);
		recordNames.add("1234567891-birch@bah.com-" + date1);
		recordNames.add("1234567892-aspen@bah.com-" + date2);
		postRecordsConnection.setRecordNames(recordNames);
		try{
			client.prepareIndex("1234567890-dogwood@bah.com-" + date0, TEST_TYPE, "1").setSource().get();
			client.prepareIndex("1234567891-birch@bah.com-" + date1, TEST_TYPE, "1").setSource().get();
			client.prepareIndex("1234567892-aspen@bah.com-" + date2, TEST_TYPE, "1").setSource().get();
			Thread.sleep(4000);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testWriteRecordMetadataToElasticsearch() throws Exception{
		postRecordsConnection.writeRecordMetadataToElasticsearch(1);
		GetResponse response = client.prepareGet(DOCUMENT_INDEX + "_1", "expire", "1").get();
		String email = response.getSourceAsMap().get("email").toString();
		assertEquals("birch@bah.com", email);
	}

	@Ignore
	@AfterClass
	public static void afterClass() throws Exception{
		client.close();
	}

}
