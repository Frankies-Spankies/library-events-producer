package franki.cursos.libraryeventsproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import franki.cursos.libraryeventsproducer.domain.LibraryEvent;
import franki.cursos.libraryeventsproducer.domain.LibraryEventType;
import franki.cursos.libraryeventsproducer.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.concurrent.ExecutionException;

@Slf4j
@RestController
public class LibraryEventsController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEventAsync2(libraryEvent);

        return new ResponseEntity<>(libraryEvent, HttpStatus.CREATED);

    }

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        if (libraryEvent.getLibraryEventId() == null){
            return new ResponseEntity<>("You must pass the Library Event Id", HttpStatus.BAD_REQUEST);
        }

        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEventProducer.sendLibraryEventAsync2(libraryEvent);

        return new ResponseEntity<>(libraryEvent, HttpStatus.OK);

    }

}
