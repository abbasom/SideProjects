package com.bah.na.asc.storage.record.management.elasticsearch;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PostNearExpiredRecords creates and posts an index onto Elasticsearch which
 * contains the meta information of records that will expire in a specified
 * number of days.
 * 
 * @author Darren Henderson
 * @version %I%, %G%
 * @since 1.0
 */
public class PostNearExpiredRecords{
	private static final Logger log = LoggerFactory.getLogger(PostNearExpiredRecords.class);

	protected static final String DOCUMENT_INDEX = "about_to_expire";
	protected static final String DOCUMENT_TYPE = "expire";

	private List<String> recordNames;
	private Client client;

	/**
	 * Construct an instance of this class.
	 * 
	 * @param esConnection
	 *            the pre-configured connection to the Elasticsearch server
	 * @since 1.0
	 */
	PostNearExpiredRecords(Client client){
		this.client = client;
		recordNames = new ArrayList<String>();
	}

	/**
	 * Specify the list of records that will expire in a specified number of
	 * days.
	 * 
	 * @param esConnection
	 *            the pre-established connection to the Elasticsearch server
	 * @since 1.0
	 */
	public void setRecordNames(List<String> recordNames){
		this.recordNames = recordNames;
	}

	/**
	 * Create and post the index containing the list of records that will expire
	 * in a specified number of days.
	 * 
	 * @param daysToExpire
	 *            the specified number of days when the list of records will
	 *            expire. Here, daysToExpire is only used to create the name of
	 *            the index to be created.
	 * @since 1.0
	 */
	public void writeRecordMetadataToElasticsearch(int daysToExpire) throws Exception{
		if((recordNames == null) || (recordNames.size() == 0)){
			log.info("There are no expiring records.");
		}
		writeToIndex(daysToExpire);
	}

	public void writeToIndex(int daysToExpire) throws Exception{
		// build up the json representation of the expired indexes
		int indexId = 1;
		createIndex(daysToExpire);
		deleteIndex(daysToExpire, DOCUMENT_INDEX + "_" + daysToExpire);
		createIndex(daysToExpire);
		BulkRequestBuilder brb = client.prepareBulk();
		for(String recordName : recordNames){
			RecordMetadata recordMetadata = new RecordMetadata(recordName);

			// make sure this is a valid record
			if((recordMetadata.getId().length() > 0) && (recordMetadata.getEmail().length() > 0)
					&& (recordMetadata.getExpires().length() > 0)){

				IndexRequestBuilder requestBuilder = client.prepareIndex(DOCUMENT_INDEX + "_" + daysToExpire,
						DOCUMENT_TYPE, String.valueOf(indexId)).setSource(
						XContentFactory.jsonBuilder().startObject().field("uid", recordMetadata.getId())
								.field("email", recordMetadata.getEmail())
								.field("expdate", recordMetadata.getExpires()).endObject());
				brb.add(requestBuilder);
				indexId++;
			}
		}

		if(brb.numberOfActions() != 0){
			// post the json to Elasticsearch
			BulkResponse response = brb.execute().actionGet();
			if(response.hasFailures()){
				log.error(response.buildFailureMessage());
			}else{
				log.info("Bulk indexing of expiring records succeeded.");
			}
		}else{
			log.info("No records to index.");
		}
	}

	public void createIndex(int daysToExpire){
		client.prepareIndex(DOCUMENT_INDEX + "_" + daysToExpire, DOCUMENT_TYPE, "1").setSource().get();

	}

	public void deleteIndex(int daysToExpire, String indexName){
		client.admin().indices().prepareDelete(indexName).get();

	}

	public void deleteExpiredIndices(int daysToExpire){
		for(String recordName : recordNames){
			deleteIndex(daysToExpire, recordName);
		}
	}

}
