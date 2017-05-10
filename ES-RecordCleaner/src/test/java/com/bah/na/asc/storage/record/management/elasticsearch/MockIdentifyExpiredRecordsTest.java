package com.bah.na.asc.storage.record.management.elasticsearch;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

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
import org.mockito.stubbing.OngoingStubbing;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bah.na.asc.storage.core.elasticsearch.ElasticsearchClient;

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
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;

import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;

public class MockIdentifyExpiredRecordsTest{
	private static final Logger logger = LoggerFactory.getLogger(MockIdentifyExpiredRecordsTest.class);
	static String date0;
	static String date1;
	static String date2;

	@InjectMocks
	private static IdentifyNearExpiredRecords expiredRecords;
	private static Client client;

	@Before
	public void beforeEachTest(){
		MockitoAnnotations.initMocks(this);
		date0 = expiredRecords.calculateFutureDate(0);
		date1 = expiredRecords.calculateFutureDate(1);
		date2 = expiredRecords.calculateFutureDate(2);
	}

	@After
	public void afterEachTest(){}

	@Test
	public void getExpiringRecords(){

		Client mockClient = mock(Client.class, RETURNS_DEEP_STUBS);
		ElasticsearchClient.setClient(mockClient);
		expiredRecords = new IdentifyNearExpiredRecords(mockClient);

		String[] badRecords = new String[]{"1234567892-aspen@bah.com-" + date2, "1234567892-birch@bah.com-" + date2};

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
		MetaData metadata = mock(MetaData.class);

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

	}

	@Test
	public void malformedIndex(){
		Client mockClient = mock(Client.class, RETURNS_DEEP_STUBS);
		ElasticsearchClient.setClient(mockClient);
		expiredRecords = new IdentifyNearExpiredRecords(mockClient);

		String[] badRecords = new String[]{"1234567892-aspen@bah.com-" + "201703-19",
				"1234567892-birch@bah.com-" + "201703-19"};

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
		MetaData metadata = mock(MetaData.class);

		when(mockClient.admin().cluster()).thenReturn(clusterAdminClient);
		when(clusterAdminClient.prepareState()).thenReturn(clusterRequestState);
		when(clusterRequestState.execute()).thenReturn(listen);
		when(listen.actionGet()).thenReturn(clusterResponse);
		when(clusterResponse.getState()).thenReturn(clusterState);
		when(clusterState.getMetaData()).thenReturn(metadata);
		when(metadata.concreteAllIndices()).thenReturn(badRecords);

		List<String> recordsList = expiredRecords.getExpiringRecords(2);

		assertEquals(0, recordsList.size());
	}

	@Test
	public void calculateFutureDate(){

		int daysToExpire = 3;
		String futureDate = expiredRecords.calculateFutureDate(daysToExpire);
		assertTrue(futureDate.toString().contains(futureDate));

	}

}
