package franki.cursos.libraryeventsproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import franki.cursos.libraryeventsproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
public class LibraryEventProducer {

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    /**
     * Se llama cuando se manda el mensaje, i.e cuando el buffer este lleno, o el linge lo indica.
     * Las funciones del calback funciona de manera asincrona con respecto a la respuesta, sin importar si la respuesta es correcta o incorrecta, i.e Una vez que se obtiene la respuesta del broker
     * se continua el thread continua con la ejecucion desde el punto donde se llamo el mensaje, y en un thread distinto y de manera asincrona
     * se ejecuta las funciones de callback.
     *
     *
     * Key en este caso se ocupa la llave LibraryEventId para determinar la particion del mensaje
     * Siendo asi que cada mensaje tenga su propia particion
     */
    public void sendLibraryEventAsync(LibraryEvent libraryEvent) throws JsonProcessingException {
        String data = objectMapper.writeValueAsString(libraryEvent.getBook());
        Integer key = libraryEvent.getLibraryEventId();

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, data);

        listenableFuture.addCallback(
                responseOnSend -> succes(key, data, responseOnSend)
                , throwable -> error(key, data, throwable)
        );

        log.info("Continua la ejecucion de sendLibraryEvent");

    }

    public ListenableFuture<SendResult<Integer, String>> sendLibraryEventAsync2(LibraryEvent libraryEvent) throws JsonProcessingException {
        String data = objectMapper.writeValueAsString(libraryEvent);
        Integer key = libraryEvent.getLibraryEventId();

        String topic = "library-events";
        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, data, topic);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);

        listenableFuture.addCallback(
                responseOnSend -> succes(key, data, responseOnSend)
                , throwable -> error(key, data, throwable)
        );

        log.info("Continua la ejecucion de sendLibraryEvent");

        return listenableFuture;

    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String data, String topic) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-sorce", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, data, recordHeaders);
    }

    /**
     * En este caso se continua con la ejecucion una ver que se obtuvo una respuesta correcta del broker
     * y si la respuesta genera un error, o excede el timeout, se maneja con un try catch
     *
     * @return
     */
    public SendResult<Integer, String> sendLibraryEventSync(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        String data = objectMapper.writeValueAsString(libraryEvent.getBook());
        Integer key = libraryEvent.getLibraryEventId();
        SendResult<Integer, String> sendResult = null;
        ;

        try {
            sendResult = kafkaTemplate.sendDefault(key, data).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException e) {
            log.error("Error ocurred: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            throw e;
        }
        return sendResult;

    }

    public void succes(Integer key, String data, SendResult<Integer, String> responseOnSend) {
        log.info("Succes response key: {}, data: {}, partition: {}", key, data, responseOnSend.getRecordMetadata().partition());
    }

    public void error(Integer key, String data, Throwable ex) {
        log.error("Error ocurred: {}", ex.getMessage());
        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Error in: {} ", throwable.getMessage());
        }
    }
}
