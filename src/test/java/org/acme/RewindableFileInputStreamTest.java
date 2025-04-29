package org.acme.util;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link RewindableFileInputStream}.
 */
public class RewindableFileInputStreamTest {

    /**
     * Helper method to create a temporary file with specified content.
     *
     * @param content Content to write into the file.
     * @return A temporary {@code File} containing the content.
     * @throws Exception If file creation fails.
     */
    private File createTempFile(String content) throws Exception {
        File temp = File.createTempFile("rewindable-test", ".tmp");
        try (FileOutputStream fos = new FileOutputStream(temp)) {
            fos.write(content.getBytes(StandardCharsets.UTF_8));
        }
        temp.deleteOnExit();
        return temp;
    }

    /**
     * Tests reset behavior when auto-rewind is enabled.
     */
    @Test
    void testAutoRewindAfterEOF() throws Exception {
        File file = createTempFile("ABCDEF");

        try (RewindableFileInputStream stream = new RewindableFileInputStream(new FileInputStream(file), true)) {
            byte[] buf = new byte[6];
            assertEquals(6, stream.read(buf));
            assertEquals("ABCDEF", new String(buf, StandardCharsets.UTF_8));

            // Reset after EOF; should rewind automatically
            stream.reset();
            buf = new byte[3];
            assertEquals(3, stream.read(buf));
            assertEquals("ABC", new String(buf, StandardCharsets.UTF_8));
        }
    }

    /**
     * Tests reset behavior when auto-rewind is disabled.
     * <p>
     * In this case:
     * - We mark() the stream BEFORE reading.
     * - We read the whole content (3 bytes, reaching EOF).
     * - We call reset(), which returns to the marked position.
     * - We should be able to read the content again.
     * </p>
     */
    @Test
    void testResetWithoutAutoRewindAfterEOF() throws Exception {
        File file = createTempFile("XYZ");

        try (RewindableFileInputStream stream = new RewindableFileInputStream(new FileInputStream(file), false)) {
            stream.mark(0);  // ➔ Mark the beginning of the file.

            byte[] buf = new byte[3];
            assertEquals(3, stream.read(buf));  // ➔ Read entire file content.
            assertEquals("XYZ", new String(buf, StandardCharsets.UTF_8));  // ➔ Confirm first read matches.

            stream.reset();  // ➔ Reset back to mark (start of file, since we marked there).

            assertEquals(3, stream.read(buf));  // ➔ Should successfully re-read 3 bytes again.
            assertEquals("XYZ", new String(buf, StandardCharsets.UTF_8));  // ➔ Confirm second read matches first read.
        }
    }


    /**
     * Tests mark and reset functionality during partial reading (before EOF).
     * <p>
     * In this case:
     * - We read part of the file (first 2 bytes).
     * - We mark() after some reading, at the middle.
     * - We read 2 more bytes (next chunk).
     * - We reset(), expecting to go back to the middle where we marked.
     * - We re-read the same chunk again.
     * </p>
     */
    @Test
    void testMarkResetDuringNormalRead() throws Exception {
        File file = createTempFile("HELLO");

        try (RewindableFileInputStream stream = new RewindableFileInputStream(new FileInputStream(file), false)) {
            byte[] buf = new byte[2];

            assertEquals(2, stream.read(buf));  // ➔ Read first 2 bytes ("HE")
            assertEquals("HE", new String(buf, StandardCharsets.UTF_8));

            stream.mark(0);  // ➔ Mark current position (after "HE").

            assertEquals(2, stream.read(buf));  // ➔ Read next 2 bytes ("LL")
            assertEquals("LL", new String(buf, StandardCharsets.UTF_8));

            stream.reset();  // ➔ Reset to marked position (after "HE").

            assertEquals(2, stream.read(buf));  // ➔ Re-read "LL" again
            assertEquals("LL", new String(buf, StandardCharsets.UTF_8));
        }
    }

}
