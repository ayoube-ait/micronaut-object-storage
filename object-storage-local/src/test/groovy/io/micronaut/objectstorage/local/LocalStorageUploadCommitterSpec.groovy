package io.micronaut.objectstorage.local

import io.micronaut.context.ApplicationContext
import io.micronaut.objectstorage.CompletedUpload
import io.micronaut.objectstorage.UploadCommitter
import io.micronaut.objectstorage.metadata.ObjectMetadataWrite
import io.micronaut.objectstorage.request.UploadRequest
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

class LocalStorageUploadCommitterSpec extends Specification {

    private Path rootDirectory
    private Path bucketPath
    private ApplicationContext context
    private LocalStorageOperations operations
    private LocalStorageObjectMetadataOperations metadataOperations

    void setup() {
        rootDirectory = Files.createTempDirectory('LocalStorageUploadCommitterSpec')
        bucketPath = rootDirectory.resolve('default')
        context = ApplicationContext.run([
            'micronaut.object-storage.local.default.path': bucketPath.toString()
        ])
        operations = context.getBean(LocalStorageOperations)
        metadataOperations = context.getBean(LocalStorageObjectMetadataOperations)
    }

    void cleanup() {
        context?.close()
        if (rootDirectory != null && Files.exists(rootDirectory)) {
            LocalStorageBucketOperations.deleteRecursively(rootDirectory)
        }
    }

    void 'streamed upload committer receives finalized metadata and returns its result'() {
        given:
        String key = 'streamed.txt'
        UploadRequest request = new UnknownLengthUploadRequest('streamed content'.bytes, key, 'text/plain', [owner: 'test'])
        CompletedUpload<LocalStorageOperations.LocalStorageFile> completedUpload = null
        UploadCommitter<LocalStorageOperations.LocalStorageFile, String> committer = { completed ->
            completedUpload = completed
            metadataOperations.save(metadataWrite(completed))
            'database-id'
        }

        when:
        def committed = operations.uploadAndCommit(request, committer)

        then:
        committed.commitResult() == 'database-id'
        committed.uploadResponse().key == key
        committed.uploadResponse().eTag == completedUpload.eTag()
        committed.uploadResponse().nativeResponse == completedUpload.nativeResponse()
        completedUpload.key() == key
        completedUpload.contentLength() == 'streamed content'.bytes.length
        completedUpload.contentType() == 'text/plain'
        completedUpload.lastModified() != null
        completedUpload.metadata() == [owner: 'test']
        completedUpload.attributes() == [:]

        and:
        def storedMetadata = metadataOperations.retrieve(key).get()
        storedMetadata.contentLength() == completedUpload.contentLength()
        storedMetadata.contentType() == completedUpload.contentType()
        storedMetadata.etag() == completedUpload.eTag()
        storedMetadata.lastModified() == completedUpload.lastModified()
    }

    void 'failed committer removes a new object and its metadata'() {
        given:
        String key = 'new-failure.txt'
        UploadCommitter<LocalStorageOperations.LocalStorageFile, String> committer = { completed ->
            metadataOperations.save(metadataWrite(completed))
            throw new IllegalStateException('commit failed')
        }

        when:
        operations.uploadAndCommit(UploadRequest.fromBytes('new'.bytes, key, 'text/plain'), committer)

        then:
        IllegalStateException e = thrown()
        e.message == 'commit failed'
        !operations.exists(key)
        !metadataOperations.retrieve(key).present
        !Files.exists(snapshotDirectory())
    }

    void 'failed replacement committer restores previous bytes and metadata'() {
        given:
        String key = 'replacement-failure.txt'
        UploadRequest original = UploadRequest.fromBytes('original'.bytes, key, 'text/plain')
        original.metadata = [version: 'original']
        operations.upload(original)
        def originalMetadata = metadataOperations.retrieve(key).get()
        UploadRequest replacement = UploadRequest.fromBytes('replacement'.bytes, key, 'application/json')
        replacement.metadata = [version: 'replacement']
        UploadCommitter<LocalStorageOperations.LocalStorageFile, String> committer = { completed ->
            metadataOperations.save(metadataWrite(completed))
            throw new IllegalStateException('commit failed')
        }

        when:
        operations.uploadAndCommit(replacement, committer)

        then:
        IllegalStateException e = thrown()
        e.message == 'commit failed'
        text(key) == 'original'
        def restoredMetadata = metadataOperations.retrieve(key).get()
        restoredMetadata.metadata() == [version: 'original']
        restoredMetadata.contentType() == originalMetadata.contentType()
        restoredMetadata.contentLength() == originalMetadata.contentLength()
        restoredMetadata.etag() == originalMetadata.etag()
        restoredMetadata.lastModified() == originalMetadata.lastModified()
        !Files.exists(snapshotDirectory())
    }

    void 'successful replacement committer commits bytes and removes snapshot state'() {
        given:
        String key = 'replacement-success.txt'
        operations.upload(UploadRequest.fromBytes('original'.bytes, key, 'text/plain'))
        UploadRequest replacement = UploadRequest.fromBytes('replacement'.bytes, key, 'text/plain')
        replacement.metadata = [version: 'replacement']
        boolean snapshotPresentDuringCommit = false
        UploadCommitter<LocalStorageOperations.LocalStorageFile, Integer> committer = { completed ->
            snapshotPresentDuringCommit = Files.list(snapshotDirectory()).withCloseable { snapshots ->
                snapshots.findAny().present
            }
            metadataOperations.save(metadataWrite(completed))
            assert metadataOperations.retrieve(key).get().etag() == completed.eTag()
            completed.contentLength() as Integer
        }

        when:
        def committed = operations.uploadAndCommit(replacement, committer)

        then:
        committed.commitResult() == 'replacement'.bytes.length
        snapshotPresentDuringCommit
        text(key) == 'replacement'
        metadataOperations.retrieve(key).get().metadata() == [version: 'replacement']
        !Files.exists(snapshotDirectory())
    }

    private String text(String key) {
        operations.retrieve(key).get().inputStream.withCloseable { input ->
            new String(input.readAllBytes(), StandardCharsets.UTF_8)
        }
    }

    private Path snapshotDirectory() {
        rootDirectory.resolve(LocalStorageOperations.INTERNAL_DIRECTORY)
            .resolve(LocalStorageOperations.SNAPSHOT_DIRECTORY)
            .resolve('default')
    }

    private static ObjectMetadataWrite metadataWrite(CompletedUpload<?> completed) {
        new ObjectMetadataWrite(
            completed.key(),
            completed.metadata(),
            completed.attributes(),
            completed.contentType(),
            completed.contentLength(),
            completed.eTag(),
            completed.lastModified()
        )
    }

    private static final class UnknownLengthUploadRequest implements UploadRequest {
        private final byte[] bytes
        private final String key
        private final String contentType
        private final Map<String, String> metadata

        private UnknownLengthUploadRequest(byte[] bytes,
                                           String key,
                                           String contentType,
                                           Map<String, String> metadata) {
            this.bytes = bytes
            this.key = key
            this.contentType = contentType
            this.metadata = metadata
        }

        @Override
        Optional<String> getContentType() {
            Optional.of(contentType)
        }

        @Override
        String getKey() {
            key
        }

        @Override
        Optional<Long> getContentSize() {
            Optional.empty()
        }

        @Override
        InputStream getInputStream() {
            new ByteArrayInputStream(bytes)
        }

        @Override
        Map<String, String> getMetadata() {
            metadata
        }
    }
}
