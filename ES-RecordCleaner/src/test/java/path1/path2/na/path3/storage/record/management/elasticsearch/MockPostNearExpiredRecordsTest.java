package com.bah.na.asc.storage.record.management.elasticsearch;

import static org.junit.Assert.*;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.any;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.stubbing.OngoingStubbing;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bah.na.asc.storage.core.elasticsearch.ElasticsearchClient;
import static org.mockito.Mockito.verify;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.aopalliance.reflect.Metadata;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import static org.mockito.Mockito.times;

public class MockPostNearExpiredRecordsTest{

	private static final Logger logger = LoggerFactory.getLogger(MockPostNearExpiredRecordsTest.class);
	static String date0;
	static String date1;
	static String date2;
	protected static final String DOCUMENT_INDEX = "about_to_expire";
	protected static final String TEST_TYPE = "tp";

	private Client mockClient = mock(Client.class, RETURNS_DEEP_STUBS);

	@InjectMocks
	@Spy
	private PostNearExpiredRecords postExpiredRecords = new PostNearExpiredRecords(mockClient);
	private IdentifyNearExpiredRecords expiredRecords = new IdentifyNearExpiredRecords(mockClient);

	@Before
	public void beforeEachTest(){
		MockitoAnnotations.initMocks(this);

	}

	@After
	public void afterEachTest(){}

	@Test
	public void testPostRecordsWriteToIndex() throws Exception{
		Client mockClient = mock(Client.class, RETURNS_DEEP_STUBS);
		ElasticsearchClient.setClient(mockClient);
		expiredRecords = new IdentifyNearExpiredRecords(mockClient);
		postExpiredRecords = new PostNearExpiredRecords(mockClient);

		date0 = expiredRecords.calculateFutureDate(0);
		date1 = expiredRecords.calculateFutureDate(1);
		date2 = expiredRecords.calculateFutureDate(2);
		String[] badRecords = new String[]{"1234567892-aspen@bah.com-" + date2, "1234567892-birch@bah.com-" + date2};
		List<String> recordNames = new ArrayList<String>();
		recordNames.add("1234567890-dogwood@bah.com-" + date0);
		recordNames.add("1234567891-birch@bah.com-" + date1);
		recordNames.add("1234567892-aspen@bah.com-" + date2);

		RecordMetadata recordMetadata = new RecordMetadata();

		recordMetadata.setId("1");
		recordMetadata.setEmail("omar");
		recordMetadata.setExpires(date2);
		IndicesExistsResponse indexResponse = new IndicesExistsResponse(false);

		AdminClient indicesAdminClient = mock(AdminClient.class);
		CreateIndexRequestBuilder createIndexRequestbuilder = mock(CreateIndexRequestBuilder.class);
		PutMappingRequestBuilder mappingBuilder = mock(PutMappingRequestBuilder.class);
		IndicesExistsRequestBuilder existRequestBuilder = mock(IndicesExistsRequestBuilder.class);
		ListenableActionFuture<ClusterStateResponse> listen = mock(ListenableActionFuture.class);
		RefreshRequestBuilder represhRequestBuilder = mock(RefreshRequestBuilder.class);
		ClusterStateRequestBuilder clusterRequestState = mock(ClusterStateRequestBuilder.class);
		ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
		ClusterStateResponse clusterResponse = mock(ClusterStateResponse.class);
		ClusterState clusterState = mock(ClusterState.class);
		IndexRequestBuilder indicesResponse = mock(IndexRequestBuilder.class);
		IndexResponse index = mock(IndexResponse.class);
		MetaData metadata = mock(MetaData.class);
		BulkRequestBuilder bulk = mock(BulkRequestBuilder.class);

		when(mockClient.admin().cluster()).thenReturn(clusterAdminClient);
		when(clusterAdminClient.prepareState()).thenReturn(clusterRequestState);
		when(clusterRequestState.execute()).thenReturn(listen);
		when(listen.actionGet()).thenReturn(clusterResponse);
		when(clusterResponse.getState()).thenReturn(clusterState);
		when(clusterState.getMetaData()).thenReturn(metadata);
		when(metadata.concreteAllIndices()).thenReturn(badRecords);

		List<String> recordsList = expiredRecords.getExpiringRecords(2);
		assertNotNull(recordsList);
		assertEquals(2, recordsList.size());
		when(
				mockClient.prepareIndex(DOCUMENT_INDEX + "_" + 2, TEST_TYPE, String.valueOf(1)).setSource(
						XContentFactory.jsonBuilder().startObject().field("uid", recordMetadata.getId())
								.field("email", recordMetadata.getEmail())
								.field("expdate", recordMetadata.getExpires()).endObject()))
				.thenReturn(indicesResponse);
		logger.info(indicesResponse.toString());

		bulk.add(indicesResponse);
		postExpiredRecords.setRecordNames(recordNames);

		postExpiredRecords.writeRecordMetadataToElasticsearch(2);

	}

	@Ignore
	@Test
	public void postExpiredRecords() throws Exception{
		Client client = mock(Client.class, RETURNS_DEEP_STUBS);
		ElasticsearchClient.setClient(client);
		PostNearExpiredRecords postExpiredRecords = new PostNearExpiredRecords(client);
		expiredRecords = new IdentifyNearExpiredRecords(mockClient);
		date0 = expiredRecords.calculateFutureDate(0);
		date1 = expiredRecords.calculateFutureDate(1);
		date2 = expiredRecords.calculateFutureDate(2);
		int indexId = 1;
		BulkAction action = BulkAction.INSTANCE;
		IndexAction indexAction = IndexAction.INSTANCE;
		BulkRequest bulkReq = mock(BulkRequest.class);
		BulkRequestBuilder bulk = new BulkRequestBuilder(client, action);

		BulkResponse bResponse = mock(BulkResponse.class);
		RecordMetadata recordMetadata = new RecordMetadata();
		recordMetadata.setEmail("omar@someEmail.com");
		recordMetadata.setId("123456");
		recordMetadata.setExpires(date2);

		List<String> recordNames = new ArrayList<String>();
		recordNames.add("1234567890-dogwood@bah.com-" + date0);
		recordNames.add("1234567891-birch@bah.com-" + date1);
		recordNames.add("1234567892-aspen@bah.com-" + date2);

		postExpiredRecords.setRecordNames(recordNames);

		int daysToExpire = 1;

		// IndexRequestBuilder indicesResponse =
		// mock(IndexRequestBuilder.class);
		ActionRequest request = mock(ActionRequest.class);
		IndexRequestBuilder bulkRes = new IndexRequestBuilder(client, indexAction);
		BulkRequestBuilder bulk2 = bulk.add(bulkRes);

		when(client.prepareBulk()).thenReturn(bulk2);
		when(
				client.prepareIndex(DOCUMENT_INDEX + "_" + daysToExpire, TEST_TYPE, String.valueOf(indexId)).setSource(
						XContentFactory.jsonBuilder().startObject().field("uid", recordMetadata.getId())
								.field("email", recordMetadata.getEmail())
								.field("expdate", recordMetadata.getExpires()).endObject())).thenReturn(bulkRes);

		postExpiredRecords.writeRecordMetadataToElasticsearch(1);
		// verify(postExpiredRecords,
		// times(1)).writeRecordMetadataToElasticsearch(1);

	}

	@Ignore
	@Test
	public void createIndex(){

		Client mockClient = mock(Client.class, RETURNS_DEEP_STUBS);
		ElasticsearchClient.setClient(mockClient);

		expiredRecords = new IdentifyNearExpiredRecords(mockClient);
		date0 = expiredRecords.calculateFutureDate(0);
		date1 = expiredRecords.calculateFutureDate(1);
		date2 = expiredRecords.calculateFutureDate(2);
		String[] badRecords = new String[]{"1234567892-aspen@bah.com-" + date2, "1234567892-birch@bah.com-" + date2};
		List<String> recordNames = new ArrayList<String>();
		recordNames.add("1234567890-dogwood@bah.com-" + date0);
		recordNames.add("1234567891-birch@bah.com-" + date1);
		recordNames.add("1234567892-aspen@bah.com-" + date2);
		postExpiredRecords.setRecordNames(recordNames);

		ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
		GetRequestBuilder getRequest = mock(GetRequestBuilder.class);
		DeleteRequestBuilder deleteRequest = mock(DeleteRequestBuilder.class);
		IndexRequestBuilder indicesResponse = mock(IndexRequestBuilder.class);
		ClusterStateResponse clusterResponse = mock(ClusterStateResponse.class);
		IndexResponse createResponmse = mock(IndexResponse.class);
		ClusterState clusterState = mock(ClusterState.class);
		MetaData metadata = mock(MetaData.class);
		int daysToExpire = 2;
		when(mockClient.prepareIndex(DOCUMENT_INDEX + "_" + daysToExpire, TEST_TYPE, "1")).thenReturn(indicesResponse);

		when(indicesResponse.setSource()).thenReturn(indicesResponse);
		when(indicesResponse.get()).thenReturn(createResponmse);
		postExpiredRecords.createIndex(2);
		verify(postExpiredRecords, times(1)).createIndex(2);

	}

	@Ignore
	@Test
	public void deleteIndex(){

		Client mockClient = mock(Client.class, RETURNS_DEEP_STUBS);
		ElasticsearchClient.setClient(mockClient);

		expiredRecords = new IdentifyNearExpiredRecords(mockClient);
		date0 = expiredRecords.calculateFutureDate(0);
		date1 = expiredRecords.calculateFutureDate(1);
		date2 = expiredRecords.calculateFutureDate(2);
		String[] badRecords = new String[]{"1234567892-aspen@bah.com-" + date2, "1234567892-birch@bah.com-" + date2};

		IndicesAdminClient indexAdminClinet = mock(IndicesAdminClient.class);
		ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
		GetRequestBuilder getRequest = mock(GetRequestBuilder.class);
		DeleteIndexRequestBuilder deleteRequest = mock(DeleteIndexRequestBuilder.class);
		IndexRequestBuilder indicesResponse = mock(IndexRequestBuilder.class);
		ClusterStateResponse clusterResponse = mock(ClusterStateResponse.class);
		IndexResponse createResponmse = mock(IndexResponse.class);
		ClusterState clusterState = mock(ClusterState.class);
		MetaData metadata = mock(MetaData.class);
		int daysToExpire = 2;

		when(mockClient.admin().indices()).thenReturn(indexAdminClinet);
		when(indexAdminClinet.prepareDelete(badRecords)).thenReturn(deleteRequest);

		postExpiredRecords.deleteIndex(2, DOCUMENT_INDEX);
		verify(postExpiredRecords, times(1)).deleteIndex(2, DOCUMENT_INDEX);

	}

	@Ignore
	@Test
	public void deleteExpiredRecords(){

		Client mockClient = mock(Client.class, RETURNS_DEEP_STUBS);
		ElasticsearchClient.setClient(mockClient);

		expiredRecords = new IdentifyNearExpiredRecords(mockClient);
		date0 = expiredRecords.calculateFutureDate(0);
		date1 = expiredRecords.calculateFutureDate(1);
		date2 = expiredRecords.calculateFutureDate(2);
		String[] badRecords = new String[]{"1234567892-aspen@bah.com-" + date2, "1234567892-birch@bah.com-" + date2};

		ClusterStateRequestBuilder clusterRequestState = mock(ClusterStateRequestBuilder.class);
		ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
		ClusterStateResponse clusterResponse = mock(ClusterStateResponse.class);
		ClusterState clusterState = mock(ClusterState.class);
		MetaData metadata = mock(MetaData.class);
		ListenableActionFuture<ClusterStateResponse> listen = mock(ListenableActionFuture.class);
		when(mockClient.admin().cluster()).thenReturn(clusterAdminClient);
		when(clusterAdminClient.prepareState()).thenReturn(clusterRequestState);
		when(clusterRequestState.execute()).thenReturn(listen);
		when(listen.actionGet()).thenReturn(clusterResponse);
		when(clusterResponse.getState()).thenReturn(clusterState);
		when(clusterState.getMetaData()).thenReturn(metadata);
		when(metadata.concreteAllIndices()).thenReturn(badRecords);
		int daysToExpire = 2;

		postExpiredRecords.deleteExpiredIndices(daysToExpire);
		verify(postExpiredRecords, times(1)).deleteExpiredIndices(daysToExpire);

	}

}
