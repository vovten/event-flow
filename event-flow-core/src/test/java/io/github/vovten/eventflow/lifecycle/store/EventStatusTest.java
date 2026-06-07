package io.github.vovten.eventflow.lifecycle.store;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("EventStatus Tests")
class EventStatusTest {

    @Test
    @DisplayName("should return UNDEFINED for code 'U'")
    void shouldReturnUndefinedForCodeU() {
        var status = EventStatus.fromCode('U');

        assertThat(status).isEqualTo(EventStatus.UNDEFINED);
    }

    @Test
    @DisplayName("should return NEW for code 'N'")
    void shouldReturnNewForCodeN() {
        var status = EventStatus.fromCode('N');

        assertThat(status).isEqualTo(EventStatus.NEW);
    }

    @Test
    @DisplayName("should return PUBLISHED for code 'P'")
    void shouldReturnPublishedForCodeP() {
        var status = EventStatus.fromCode('P');

        assertThat(status).isEqualTo(EventStatus.PUBLISHED);
    }

    @Test
    @DisplayName("should return HANDLED for code 'H'")
    void shouldReturnHandledForCodeH() {
        var status = EventStatus.fromCode('H');

        assertThat(status).isEqualTo(EventStatus.HANDLED);
    }

    @Test
    @DisplayName("should return FAILED for code 'F'")
    void shouldReturnFailedForCodeF() {
        var status = EventStatus.fromCode('F');

        assertThat(status).isEqualTo(EventStatus.FAILED);
    }

    @Test
    @DisplayName("should throw IllegalArgumentException for unknown code")
    void shouldThrowForUnknownCode() {
        assertThatThrownBy(() -> EventStatus.fromCode('X'))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown status code: X");
    }

    @Test
    @DisplayName("should throw IllegalArgumentException for lowercase code")
    void shouldThrowForLowercaseCode() {
        assertThatThrownBy(() -> EventStatus.fromCode('n'))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("should return correct code character for each status")
    void shouldReturnCorrectCodeCharacter() {
        assertThat(EventStatus.UNDEFINED.getCode()).isEqualTo('U');
        assertThat(EventStatus.NEW.getCode()).isEqualTo('N');
        assertThat(EventStatus.PUBLISHED.getCode()).isEqualTo('P');
        assertThat(EventStatus.HANDLED.getCode()).isEqualTo('H');
        assertThat(EventStatus.FAILED.getCode()).isEqualTo('F');
    }
}
