package franki.cursos.libraryeventsproducer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import franki.cursos.libraryeventsproducer.domain.Book;
import franki.cursos.libraryeventsproducer.domain.LibraryEvent;
import franki.cursos.libraryeventsproducer.producer.LibraryEventProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

import static org.junit.jupiter.api.Assertions.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
class LibraryEventsControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    @MockBean
    LibraryEventProducer libraryEventProducer;

    ObjectMapper objectMapper = new ObjectMapper();

    Book book;

    LibraryEvent libraryEvent;

    private static final String host = "http://localhost";
    private static final String uri = "/v1/libraryevent";

    @Test
    void postLibraryEvent() throws Exception {
        ///Given
        book = Book.builder()
                .bookId(123)
                .bookAuthor("Eric Evans")
                .bookName("DDD")
                .build();

        libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        when(libraryEventProducer.sendLibraryEventAsync2(libraryEvent)).thenReturn(null);

        //When
        ResultActions perform = mockMvc.perform(post(host + uri)
                .content(objectMapper.writeValueAsString(libraryEvent))
                .contentType(MediaType.APPLICATION_JSON));

        //Then
        perform.andExpect(status().isCreated());
    }

    @Test
    void postLibraryEvent_4xx() throws Exception {
        ///Given
        book = Book.builder()
                .bookId(null)
                .bookAuthor(null)
                .bookName("DDD")
                .build();
        
        libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(null)
                .book(book)
                .build();

        when(libraryEventProducer.sendLibraryEventAsync2(libraryEvent)).thenReturn(null);

        //When
        ResultActions perform = mockMvc.perform(post(host + uri)
                .content(objectMapper.writeValueAsString(libraryEvent))
                .contentType(MediaType.APPLICATION_JSON));


        //Then
        String expectedErrorMessage = "book.bookAuthor - must not be blank, book.bookId - must not be null";
        perform.andExpect(status().is4xxClientError())
        .andExpect(content().string(expectedErrorMessage));
    }
}