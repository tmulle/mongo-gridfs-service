package org.acme;

import org.acme.util.FileHasher;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link FileHasher}.
 */
public class FileHasherTest {

    /**
     * Helper method to create a temporary file with specified content.
     *
     * @param content Content to write into the file.
     * @return A temporary {@code File} containing the content.
     * @throws Exception If file creation fails.
     */
    private File createTempFile(String content) throws Exception {
        File temp = File.createTempFile("filehasher-test", ".tmp");
        try (FileOutputStream fos = new FileOutputStream(temp)) {
            fos.write(content.getBytes(StandardCharsets.UTF_8));
        }
        temp.deleteOnExit();
        return temp;
    }

    /**
     * Tests computing a SHA-256 hash with the default chunk size.
     */
    @Test
    void testSha256WithDefaultChunkSize() throws Exception {
        String content = "Hello World";
        String expectedSha256Hex = "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e";

        File file = createTempFile(content);
        try (FileChannel channel = FileChannel.open(file.toPath())) {
            String actualHash = FileHasher.computeHash(channel, "SHA-256");
            assertEquals(expectedSha256Hex, actualHash, "SHA-256 hash mismatch");
        }
    }

    /**
     * Tests computing a SHA-256 hash with a custom small chunk size (e.g., 16KB).
     */
    @Test
    void testSha256WithCustomChunkSize() throws Exception {
        String content = "Hello World";
        String expectedSha256Hex = "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e";

        File file = createTempFile(content);
        try (FileChannel channel = FileChannel.open(file.toPath())) {
            String actualHash = FileHasher.computeHash(channel, "SHA-256", 16 * 1024); // 16KB
            assertEquals(expectedSha256Hex, actualHash, "SHA-256 hash mismatch with custom chunk size");
        }
    }

    /**
     * Tests that an invalid algorithm name throws {@code IllegalArgumentException}.
     */
    @Test
    void testInvalidAlgorithmThrowsException() throws Exception {
        String content = "SomeContent";

        File file = createTempFile(content);
        try (FileChannel channel = FileChannel.open(file.toPath())) {
            assertThrows(IllegalArgumentException.class, () -> {
                FileHasher.computeHash(channel, "NON_EXISTENT_ALGO");
            }, "Expected IllegalArgumentException for unsupported algorithm");
        }
    }

    /**
     * Tests that providing a non-positive chunk size throws {@code IllegalArgumentException}.
     */
    @Test
    void testInvalidChunkSizeThrowsException() throws Exception {
        String content = "AnotherContent";

        File file = createTempFile(content);
        try (FileChannel channel = FileChannel.open(file.toPath())) {
            assertThrows(IllegalArgumentException.class, () -> {
                FileHasher.computeHash(channel, "SHA-256", 0); // Invalid chunk size
            }, "Expected IllegalArgumentException for non-positive chunk size");
        }
    }

    /**
     * Tests that the algorithm support check works correctly.
     */
    @Test
    void testIsAlgorithmSupported() {
        assertTrue(FileHasher.isAlgorithmSupported("SHA-256"), "SHA-256 should be supported");
        assertFalse(FileHasher.isAlgorithmSupported("FOO-123"), "FOO-123 should not be supported");
    }
}
