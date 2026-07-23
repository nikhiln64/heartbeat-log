package heartbeatlog.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntryTest {

    @Test
    void entryCarriesEpochOffsetPayload() {
        Entry e = new Entry(2, 5, "hr=61,hrv=48");
        assertEquals(2, e.epoch());
        assertEquals(5, e.offset());
        assertEquals("hr=61,hrv=48", e.payload());
    }

    @Test
    void rejectsNegativeEpochNegativeOffsetAndNullPayload() {
        assertThrows(IllegalArgumentException.class, () -> new Entry(-1, 0, "x"));
        assertThrows(IllegalArgumentException.class, () -> new Entry(0, -1, "x"));
        assertThrows(IllegalArgumentException.class, () -> new Entry(0, 0, null));
    }
}
