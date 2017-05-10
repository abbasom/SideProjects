package com.bah.na.asc.storage.record.management.elasticsearch;

//this class is commented out because we dont use it anywhere 
import java.net.UnknownHostException;

import java.util.List;

import org.elasticsearch.client.Client;

//import org.elasticsearch.action.index.IndexResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bah.na.asc.core.system.EnvProperty;
import com.bah.na.asc.storage.core.elasticsearch.ElasticsearchClient;

public class ESRecordManager{
	private static final Logger log = LoggerFactory.getLogger(ESRecordManager.class);

	static String ASC_ES_IP = EnvProperty.getInstance().getEnvVar("ASC_ES_IP");
	static String ASC_ES_TRANSPORT_PORT = EnvProperty.getInstance().getEnvVar("ASC_ES_TRANSPORT_PORT");
	static String ASC_ES_CLUSTER_NAME = EnvProperty.getInstance().getEnvVar("ASC_ES_CLUSTER_NAME");

	public static void main(String[] args) throws UnknownHostException{

		int daysToExpire0 = 0;
		int daysToExpire1 = 1;
		int daysToExpire2 = 2;

		Client client = null;

		try{
			client = ElasticsearchClient.getElasticsearchClient(ASC_ES_CLUSTER_NAME, ASC_ES_IP,
					Integer.parseInt(ASC_ES_TRANSPORT_PORT));
		}catch(UnknownHostException uhe){
			throw uhe;
		}catch(Exception e){
			throw e;
		}
		log.info("Identifying indices that expire in zero days");
		deleteIndex(client, daysToExpire0);
		log.info("Identifying indices that expire in one day");
		populateIndex(client, daysToExpire1);
		log.info("Identifying indices that expire in two days");
		populateIndex(client, daysToExpire2);

	}

	public static void deleteIndex(Client connection, int daysToExpire){
		IdentifyNearExpiredRecords identifyNearExpiredRecords = new IdentifyNearExpiredRecords(connection);
		List<String> listExpiringRecords = identifyNearExpiredRecords.getExpiringRecords(daysToExpire);

		PostNearExpiredRecords postNearExpiredRecords = new PostNearExpiredRecords(connection);
		postNearExpiredRecords.setRecordNames(listExpiringRecords);

		try{
			postNearExpiredRecords.deleteExpiredIndices(daysToExpire);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public static void populateIndex(Client connection, int daysToExpire){
		IdentifyNearExpiredRecords identifyNearExpiredRecords = new IdentifyNearExpiredRecords(connection);
		List<String> listExpiringRecords = identifyNearExpiredRecords.getExpiringRecords(daysToExpire);

		PostNearExpiredRecords postNearExpiredRecords = new PostNearExpiredRecords(connection);
		postNearExpiredRecords.setRecordNames(listExpiringRecords);

		try{
			postNearExpiredRecords.writeRecordMetadataToElasticsearch(daysToExpire);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
