package franki.cursos.libraryeventsproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import franki.cursos.libraryeventsproducer.domain.Book;
import franki.cursos.libraryeventsproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.client.RestTemplate;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Exchanger;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@Slf4j
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.bootstrap.servers=${spring.embedded.kafka.brokers}"})
@SpringBootTest
@AutoConfigureMockMvc
class LibraryEventsControllerTest {

    @Autowired
    MockMvc mockMvc;

    LibraryEvent libraryEvent;

    @Autowired
    ObjectMapper mapper;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    RestTemplateBuilder restTemplateBuilder;

    private Consumer<Integer,String> consumer;

    @BeforeEach
    void setUp() {
        Map<String,Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);

        libraryEvent = LibraryEvent.builder()
                .book(Book.builder()
                        .bookAuthor("Eric evans")
                        .bookId(123).bookName("DDD")
                        .build())
                .build();
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void postLibraryEvent() throws Exception {
        String libraryEventBody = mapper.writeValueAsString(libraryEvent);

        ResultActions perform = mockMvc.perform(post("/v1/libraryevent")
                .contentType(MediaType.APPLICATION_JSON)
                .content(libraryEventBody));

        String contentAsString = perform.andReturn().getResponse().getContentAsString();
        log.info("Result of /v1/libraryevent: " + contentAsString);
        perform.andExpect(status().isCreated());

        ConsumerRecord<Integer, String> consumerRecord =  KafkaTestUtils.getSingleRecord(consumer,"library-events");
        Thread.sleep(3000);
        String expectedRecord ="{\"libraryEventId\":null,\"book\":{\"bookId\":123,\"bookName\":\"DDD\",\"bookAuthor\":\"Eric evans\"},\"libraryEventType\":\"NEW\"}";
        String value = consumerRecord.value();
        assertEquals(expectedRecord, value);

    }

    @Test
    void putLibraryEvent() throws Exception {
        libraryEvent.setLibraryEventId(123);
        String libraryEventBody = mapper.writeValueAsString(libraryEvent);

        ResultActions perform = mockMvc.perform(put("/v1/libraryevent")
                .contentType(MediaType.APPLICATION_JSON)
                .content(libraryEventBody));

        String contentAsString = perform.andReturn().getResponse().getContentAsString();
        log.info("Result of /v1/libraryevent: " + contentAsString);
        perform.andExpect(status().isOk());

        ConsumerRecord<Integer, String> consumerRecord =  KafkaTestUtils.getSingleRecord(consumer,"library-events");
        Thread.sleep(3000);
        String expectedRecord ="{\"libraryEventId\":123,\"book\":{\"bookId\":123,\"bookName\":\"DDD\",\"bookAuthor\":\"Eric evans\"},\"libraryEventType\":\"UPDATE\"}";
        String value = consumerRecord.value();
        assertEquals(expectedRecord, value);


    }
}