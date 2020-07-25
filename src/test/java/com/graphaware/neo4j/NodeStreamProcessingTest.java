package com.graphaware.neo4j;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Values;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NodeStreamProcessingTest {

	private static final Logger LOG = LoggerFactory.getLogger(NodeStreamProcessingTest.class);

	private static final String TEST_LABEL = "Test";
	private static final String ADDITIONAL_LABEL = "NewLabel";
	private static final String DB_NAME = "myTestDb";
	private static final int WRITER_THREAD_COUNT = 8;
	private static final int NODES_COUNT = 10000;  // make sure to change this also in the data creation query
	private static final Config DRIVER_CONFIG = Config.builder().build();
	private Driver neo4jDriver;

	@BeforeAll
	void setUp() {
		neo4jDriver = GraphDatabase.driver("bolt://localhost:8687", AuthTokens.none(), DRIVER_CONFIG);
	}

	@AfterAll
	void afterAll() {
		neo4jDriver.closeAsync();
	}

	@BeforeEach
	void prepareTestDatabase() {
		LOG.info("Creating test DB");
		neo4jDriver.session(SessionConfig.forDatabase("system")).run("CREATE OR REPLACE DATABASE " + DB_NAME).consume();
		LOG.info("Created test database: {}", DB_NAME);
		// note that nodes are created with a 10k data payload, to put some pressure on memory
		// and the test memory configuration is limited to 32 MB
		neo4jDriver.session(SessionConfig.forDatabase(DB_NAME)).run("""
				CALL apoc.periodic.iterate(
					"UNWIND range(1, 10000) as i RETURN i", 
					"CREATE (n:Test{data: apoc.text.random(10000)})", {batchSize:100})
				YIELD batches, total return batches, total"""
				).consume();
		// unfortunately, we cannot easily parameterize the apoc statement with node label and count with new java text blocks
		LOG.info("Created test nodes");
	}

	@Test
	void copyAllNodes() {

		long startTime = System.currentTimeMillis();

		Integer processedCount = readNodes()
				.doOnEach(nodeSignal -> System.out.print("r"))
				.flatMap(this::processNode, WRITER_THREAD_COUNT)
				.reduce(0, (count, result) -> count + result.counters().labelsAdded())
				.block();

		assertEquals(NODES_COUNT, processedCount);

		int nodeCount = neo4jDriver.session(SessionConfig.forDatabase(DB_NAME))
				.run("MATCH (n:" + ADDITIONAL_LABEL + ") RETURN count(n)as cnt")
				.single().get("cnt").asInt();

		assertEquals(NODES_COUNT, nodeCount);

		System.out.println('\n');
		LOG.info("Finished in {} ms", System.currentTimeMillis() - startTime);
	}

	private Flux<Node> readNodes() {
		return Flux.usingWhen(Mono.fromSupplier(getRxSession()),
				session -> session.readTransaction(tx -> {
					RxResult result = tx.run("MATCH (n:" + TEST_LABEL + ") RETURN n");
					return Flux.from(result.records())
							.flatMap(record -> Mono.just(record.get(0).asNode()));
				})
				, RxSession::close)
			.doOnComplete(() -> System.out.print("<Reading_complete>"));
	}

	private Flux<ResultSummary> processNode(Node node) {
		return Flux.usingWhen(Mono.fromSupplier(getRxSession()),
				session -> session.writeTransaction(tx -> tx.run("MATCH (n) WHERE id(n)=$nodeId SET n:" + ADDITIONAL_LABEL,
						Values.parameters("nodeId", Values.value(node.id()))).consume())
				, RxSession::close)
				.doOnNext(it -> System.out.print("W"));
	}

	private Supplier<RxSession> getRxSession() {
		return () -> neo4jDriver.rxSession(SessionConfig.forDatabase(DB_NAME));
	}
}
