package franki.cursos.libraryeventsproducer.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent {

    private Integer libraryEventId;
    @NotNull
    @Valid //Es nesesaria esta anotacion para que se validen los campos de las depencencias de esta clase, cuando se valida esta clase
    private Book book;
    private LibraryEventType libraryEventType;
}
