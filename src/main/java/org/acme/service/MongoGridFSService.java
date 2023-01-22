package org.acme.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.acme.exceptions.InvalidRequestException;
import org.acme.util.MarkableFileInputStream;
import org.apache.commons.codec.digest.DigestUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Service which communicates with a MongoGridFs server to store/retrieve files
 *
 * @author tmulle
 */
public class MongoGridFSService {

    private final static Logger LOG = LoggerFactory.getLogger(MongoGridFSService.class);
    // Holds the database name
    private final String databaseName;
    // Holds the files bucket name
    private final String bucketName;
    // Holds the chunk size to send files
    private final Integer chunkSize;
    // Holds the MongoClient
    private final MongoClient client;
    // Holds the database
    private final MongoDatabase database;
    // Communicates with the Grid itself
    private GridFSBucket gridFSBucket;

    /**
     * Constructor
     *
     * @param databaseName   Database name
     * @param fileBucketName Name of the bucket to use for files
     * @param fileChunkSize  Size of chunks to split file
     * @param client         MongoDb Client
     */
    public MongoGridFSService(String databaseName,
                              String fileBucketName,
                              int fileChunkSize,
                              MongoClient client) {

        database = client.getDatabase(databaseName);
        if (fileBucketName != null && !fileBucketName.isEmpty()) {
            gridFSBucket = GridFSBuckets.create(database, fileBucketName);
            this.bucketName = fileBucketName;
        } else {
            gridFSBucket = GridFSBuckets.create(database);
            this.bucketName = gridFSBucket.getBucketName();
        }

        this.chunkSize = fileChunkSize;
        this.databaseName = databaseName;
        this.client = client;
    }

    /**
     * Upload to Grid
     * <p>
     * We automatically calculate a SHA-256 hash on the file during
     * upload and store it in the metadata as "sha256"
     *
     * @param file     Path of the file
     * @param metaData Extra information stored with the file
     * @return ObjectId of new file
     */
    public ObjectId uploadFile(Path file, String fileName, Map<String, Object> metaData) {
        Objects.requireNonNull(file, "Path is required");
        Objects.requireNonNull(fileName, "Filename is required");

        try {
            MarkableFileInputStream fileInputStream = new MarkableFileInputStream(new FileInputStream(file.toFile()));
            return uploadFile(fileInputStream, fileName, metaData);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found: " + file.toFile(), e);
        }

    }


    /**
     * Upload file to Mongo GridFS
     * <p>
     * We automatically generate a sha-256 hash and store it in the meta data
     * under the key "sha256"
     *
     * @param inputStream Inputstream to the data
     * @param fileName
     * @param metaData    and extran information you want to store
     * @return ObjectId of uploaded file
     */
    public ObjectId uploadFile(MarkableFileInputStream inputStream, String fileName, Map<String, Object> metaData) {

        Objects.requireNonNull(inputStream, "MarkableFileInputStream is required");
        Objects.requireNonNull(fileName, "Filename is required");


        // Generate a hash and store in metadata
        String sha256 = null;
        try {
            sha256 = DigestUtils.sha256Hex(inputStream);
            inputStream.mark(0); //reset stream
            inputStream.reset();
        } catch (IOException ex) {
            throw new RuntimeException("Error generating hash for file", ex);
        }

        // Check if we already have the hash
        // if so return error
        if (hashExists(sha256))
            throw new InvalidRequestException("A documents already exists with hash: " + sha256);

        // We are unique
        // Create the meta
        Document metaDoc = new Document();
        metaDoc.append("sha256", sha256);

        // Add any meta information
        // we don't allow the sha256 to be overriden
        if (metaData != null) {
            metaData.entrySet().forEach(entry -> {
                if (!"sha256".equalsIgnoreCase(entry.getKey())) {
                    metaDoc.append(entry.getKey(), entry.getValue());
                }
            });
        }

        try {
            // Create the options
            GridFSUploadOptions options = new GridFSUploadOptions();
            if (chunkSize != null) {
                options = options.chunkSizeBytes(chunkSize);
            }
            options = options.metadata(metaDoc);

            // Upload to server
            return gridFSBucket.uploadFromStream(fileName, inputStream, options);
        } catch (Exception e) {
            throw new RuntimeException("Error uploading file", e);
        }
    }

    /**
     * Return a list of FileInfo for each record in the Grid
     *
     * @return List of FileInfo objects or empty
     */
    public List<FileInfo> listAllFiles(Map<String, String> queryParams) {

        // Holds the results
        List<FileInfo> items = new ArrayList<>();

        // Default empty query used when there are no params
        Bson query = Filters.empty();

        // Holds the defaults for the incoming parameters
        int recordLimit = 0;
        int skipRecord = 0;
        String sortDir = null;
        List<String> sortByFields = null;

        // Build up query from the params
        if (queryParams != null || queryParams.isEmpty()) {

            // Holds all the conditions
            List<Bson> filters = new ArrayList<>();

            // Parse the ticketnumber
            parseTicketNumber(queryParams, filters);

            // Date parsing
            parseDateParams(queryParams, filters);

            // read any limit
            recordLimit = parseRecordLimit(queryParams);

            // read any skip count
            skipRecord = parseSkipCount(queryParams);

            // Sorting
            sortByFields = parseSortingFields(queryParams);

            // Sort direction
            sortDir = parseSortingDirection(queryParams);

            // filename
            parseFilename(queryParams,filters);

            // Build the filter chain
            if (!filters.isEmpty()) {
                query = Filters.and(filters);
            }
        }

        // run the query
        GridFSFindIterable gridFSFiles = gridFSBucket.find(query);

        // Are we limiting the return size
        if (recordLimit > 0) {
            gridFSFiles.limit(recordLimit);
        }

        // Skipping any records?
        if (skipRecord > 0) {
            gridFSFiles.skip(skipRecord);
        }

        // if we have any sorting
        if (sortDir != null && !sortByFields.isEmpty()) {
            if ("ASC".equals(sortDir)) {
                gridFSFiles.sort(Sorts.ascending(sortByFields));
            } else {
                gridFSFiles.sort(Sorts.descending(sortByFields));
            }
        }

        LOG.debug("Running query with Filters: {} and Sorting: {} - {}", query, sortDir, sortByFields);
        gridFSFiles.forEach(file -> {
            FileInfo info = new FileInfo();
            info.filename = file.getFilename();
            info.length = file.getLength();
            info.uploadDate = file.getUploadDate();
            info.id = file.getObjectId().toString();
            info.metaData = file.getMetadata();
            items.add(info);
        });

        return items;
    }

    /**
     * Add filename filter if requested
     *
     * @param queryParams Incoming params
     * @param filters Filter list to add filter
     */
    private void parseFilename(Map<String, String> queryParams, List<Bson> filters) {
        String filename = queryParams.get("filename");
        if (filename != null && !filename.isEmpty()) {
            filters.add(Filters.eq("filename", filename));
        }
    }


    /**
     * Delete a file from the Grid
     *
     * @param id
     */
    public void deleteFile(String id) {
        Objects.requireNonNull(id, "Id is required");

        // Perform check so we can throw our own exception
        // Mongo only throws a generic  com.mongodb.MongoGridFSException
        // with string messaes and I want to keep out exceptions consistent
        // with simplified wording
        if (!fileIDExists(id)) {
            throw new InvalidRequestException("ID " + id + " does not exist");
        }

        // Delete the file
        gridFSBucket.delete(parseObjectId(id));
    }

    /**
     * Download the file by ObjectID
     *
     * @param id           ObjectId string
     * @param outputStream OutputStream to write the data
     */
    public void downloadFile(String id, OutputStream outputStream) {
        Objects.requireNonNull(id, "Id is required");
        Objects.requireNonNull(outputStream, "OutputStream is required");

        // Perform check so we can throw our own exception
        // Mongo only throws a generic  com.mongodb.MongoGridFSException
        // with string messaes and I want to keep out exceptions consistent
        // with simplified wording
        if (!fileIDExists(id)) {
            throw new InvalidRequestException("ID " + id + " does not exist");
        }

        // Download the stream
        gridFSBucket.downloadToStream(parseObjectId(id), outputStream);
    }

    /**
     * Get file info for a single record
     *
     * @param id ObjectId hash
     * @return Optional with a FileInfo
     */
    public FileInfo getFileInfo(String id) {
        Objects.requireNonNull(id, "Id is required");

        Bson bson = Filters.eq("_id", parseObjectId(id));
        GridFSFile file = gridFSBucket.find(bson).first();

        // If not found, throw
        if (file == null) {
            throw new InvalidRequestException("ID " + id + " does not exist");
        }

        // Build the info
        FileInfo info = new FileInfo();
        info.filename = file.getFilename();
        info.length = file.getLength();
        info.uploadDate = file.getUploadDate();
        info.id = file.getObjectId().toString();
        info.metaData = file.getMetadata();

        return info;

    }

    /**
     * Returns if a hash is already stored in the db
     *
     * @param hash SHA256 hash
     * @return true or false
     */
    public boolean hashExists(String hash) {
        Objects.requireNonNull(hash, "Hash is required");
        long count = database.getCollection(bucketName + ".files").countDocuments(Filters.eq("metadata.sha256", hash));
        return count > 0;
    }

    /**
     * Returns if a file id is already stored in the db
     *
     * @param id ObjectId
     * @return true or false
     */
    public boolean fileIDExists(String id) {
        Objects.requireNonNull(id, "ID is required");
        ObjectId objectId = parseObjectId(id);
        long count = database.getCollection(bucketName + ".files")
                .countDocuments(Filters.eq("_id", objectId));
        return count > 0;
    }

    /**
     * Returns if a filename is already stored in the db
     *
     * @param filename Name of the file
     * @return true or false
     */
    public boolean filenameExists(String filename) {
        Objects.requireNonNull(filename, "Filename is required");
        long count = database.getCollection(bucketName + ".files")
                .countDocuments(Filters.eq("filename", filename));
        return count > 0;
    }

    /**
     * Get the total file count
     *
     * @return
     */
    public long totalFileCount() {
        return database.getCollection(bucketName + ".files").countDocuments();
    }


    /**
     * Returns a new ObjectID
     *
     * @param id
     * @return ObjectId
     * @throws InvalidRequestException if can't be parsed
     */
    private ObjectId parseObjectId(String id) {
        try {
            return new ObjectId(id);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Invalid format for id - " + id, e);
        }
    }

    /**
     * Parse the sorting direction
     *
     * @param queryParams
     * @return
     */
    private String parseSortingDirection(Map<String, String> queryParams) {
        String sortDirParam = queryParams.get("sortDir");
        String sortDir = null;
        if (sortDirParam != null && !sortDirParam.isEmpty()) {
            if ("ASC".equalsIgnoreCase(sortDirParam)) {
                sortDir = "ASC";
            } else if ("DESC".equalsIgnoreCase(sortDirParam)) {
                sortDir = "DESC";
            }
        }
        return sortDir;
    }

    /**
     * Parse the sorting fields
     *
     * @param queryParams
     * @return
     */
    private List<String> parseSortingFields(Map<String, String> queryParams) {
        String sortFields = queryParams.get("sortFields");
        List<String> sortByFields = null;

        if (sortFields != null && !sortFields.isEmpty()) {
            String[] strings = sortFields.split(",");

            // If we have fields specified
            // need to rewrite the "id" column to internal "_id"
            // all others we pass through
            if (strings.length > 0) {
                sortByFields = new ArrayList<>();
                for (String s : strings) {
                    if ("id".equalsIgnoreCase(s)) {
                        sortByFields.add("_id");
                    } else {
                        sortByFields.add(s);
                    }
                }
            }
        }
        return sortByFields;
    }

    /**
     * Parse skip count
     *
     * @param queryParams
     * @return
     */
    private int parseSkipCount(Map<String, String> queryParams) {
        String skipParam = queryParams.get("skip");
        int skipRecord = 0;
        if (skipParam != null && !skipParam.isEmpty()) {
            try {
                skipRecord = Integer.parseInt(skipParam);
            } catch (NumberFormatException nfe) {
                throw new InvalidRequestException("skip param must be a whole number");
            }
        }
        return skipRecord;
    }

    /**
     * Parse record limit
     *
     * @param queryParams
     * @return
     */
    private int parseRecordLimit(Map<String, String> queryParams) {
        String limitParam = queryParams.get("limit");
        int recordLimit = 0;
        if (limitParam != null && !limitParam.isEmpty()) {
            try {
                recordLimit = Integer.parseInt(limitParam);
            } catch (NumberFormatException nfe) {
                throw new InvalidRequestException("limit param must be a whole number");
            }
        }
        return recordLimit;
    }

    /**
     * Parse date params
     *
     * @param queryParams
     * @param filters
     */
    private void parseDateParams(Map<String, String> queryParams, List<Bson> filters) {
        String startDate = queryParams.get("startDate");
        String endDate = queryParams.get("endDate");

        if (startDate != null || endDate != null) {
            // Convert to date objects
            SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy");
            sdf.setLenient(false);

            // Start Date
            if (startDate != null) {
                Date start = null;
                try {
                    start = sdf.parse(startDate);
                } catch (ParseException e) {
                    throw new InvalidRequestException("Cannot parse startDate");
                }
                filters.add(Filters.gte("uploadDate", start));
            }

            // End Date
            if (endDate != null) {
                Date end = null;
                try {
                    end = sdf.parse(endDate);
                } catch (ParseException e) {
                    throw new InvalidRequestException("Cannot parse endDate");
                }
                filters.add(Filters.lte("uploadDate", end));
            }
        }
    }

    /**
     * Parse ticket number
     *
     * @param queryParams
     * @param filters
     */
    private void parseTicketNumber(Map<String, String> queryParams, List<Bson> filters) {
        // Check for ticketNumber
        String ticketNumber = queryParams.get("ticketNumber");
        if (ticketNumber != null && !ticketNumber.isEmpty()) {
            filters.add(Filters.eq("metadata.ticketNumber", ticketNumber));
        }
    }

    /**
     * Info class to map from a GridFSFile to POJO
     */
    @NoArgsConstructor
    @Getter
    public static class FileInfo {

        String id;

        String filename;
        Long length;
        Date uploadDate;

        Map<String, Object> metaData;

    }
}
