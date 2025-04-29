package org.acme.util;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * {@code FileHasher} provides utility methods to compute cryptographic hashes
 * (e.g., SHA-256, SHA-512, SHA-1) over file contents using efficient memory-mapped IO.
 * <p>
 * Supports any digest algorithm available in {@link MessageDigest}.
 * </p>
 */
public class FileHasher {

    private static final long DEFAULT_MAP_CHUNK_SIZE = 128L * 1024L * 1024L; // 128MB

    private FileHasher() {
        // Utility class - no instantiation
    }

    /**
     * Computes the hash of a file using the specified algorithm with default chunk size.
     *
     * @param fileChannel  The {@code FileChannel} to hash.
     * @param algorithmName The digest algorithm name (e.g., "SHA-256", "SHA-512").
     * @return Lowercase hexadecimal string of the file's hash.
     * @throws IOException If an IO error occurs.
     * @throws IllegalArgumentException If the algorithm is not supported.
     */
    public static String computeHash(FileChannel fileChannel, String algorithmName) throws IOException {
        return computeHash(fileChannel, algorithmName, DEFAULT_MAP_CHUNK_SIZE);
    }

    /**
     * Computes the hash of a file using the specified algorithm and custom chunk size.
     *
     * @param fileChannel   The {@code FileChannel} to hash.
     * @param algorithmName The digest algorithm name (e.g., "SHA-256", "SHA-512").
     * @param mapChunkSize  Size of each memory-mapped block in bytes.
     * @return Lowercase hexadecimal string of the file's hash.
     * @throws IOException If an IO error occurs.
     * @throws IllegalArgumentException If algorithm unsupported or chunk size invalid.
     */
    public static String computeHash(FileChannel fileChannel, String algorithmName, long mapChunkSize) throws IOException {
        if (mapChunkSize <= 0) {
            throw new IllegalArgumentException("mapChunkSize must be positive");
        }

        try {
            MessageDigest digest = MessageDigest.getInstance(algorithmName);

            long fileSize = fileChannel.size();
            long position = 0;

            while (position < fileSize) {
                long remaining = fileSize - position;
                long bytesToMap = Math.min(remaining, mapChunkSize);

                MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, bytesToMap);
                digest.update(buffer);

                position += bytesToMap;
            }

            return HexFormat.of().formatHex(digest.digest());

        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Unsupported hash algorithm: " + algorithmName, e);
        }
    }

    /**
     * Checks if a digest algorithm is available on this JVM.
     *
     * @param algorithmName Name of the algorithm to check.
     * @return {@code true} if available, {@code false} otherwise.
     */
    public static boolean isAlgorithmSupported(String algorithmName) {
        try {
            MessageDigest.getInstance(algorithmName);
            return true;
        } catch (NoSuchAlgorithmException e) {
            return false;
        }
    }
}
