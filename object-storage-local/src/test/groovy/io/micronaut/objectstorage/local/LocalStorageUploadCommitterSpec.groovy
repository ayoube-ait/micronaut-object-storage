package io.micronaut.objectstorage.local

import io.micronaut.context.ApplicationContext
import io.micronaut.objectstorage.CompletedUpload
import io.micronaut.objectstorage.ObjectStorageException
import io.micronaut.objectstorage.UploadCommitter
import io.micronaut.objectstorage.request.UploadRequest
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.nio.file.DirectoryNotEmptyException
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

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

    void 'streamed upload committer receives finalized storage information without sidecars'() {
        given:
        String key = 'streamed.txt'
        byte[] bytes = 'streamed content'.bytes
        CloseTrackingInputStream inputStream = new CloseTrackingInputStream(bytes)
        UploadRequest request = UploadRequest.fromInputStream(inputStream, key, 'text/plain', bytes.length + 100L)
        request.metadata = [owner: 'application']
        AtomicInteger callbackCalls = new AtomicInteger()
        Map<String, CompletedUpload<LocalStorageOperations.LocalStorageFile>> externalMetadata = new ConcurrentHashMap<>()
        UploadCommitter<LocalStorageOperations.LocalStorageFile, String> committer = { completed ->
            callbackCalls.incrementAndGet()
            externalMetadata.put(key, completed)
            'database-id'
        }

        when:
        def committed = operations.uploadAndCommit(request, committer)

        then:
        callbackCalls.get() == 1
        committed.commitResult() == 'database-id'
        committed.uploadResponse().key == key
        def completed = externalMetadata.get(key)
        committed.uploadResponse().eTag == completed.eTag()
        committed.uploadResponse().nativeResponse == completed.nativeResponse().get()
        completed.key() == key
        completed.contentLength() == bytes.length
        completed.contentType() == 'text/plain'
        completed.storageTimestamp() != null
        completed.metadata() == [owner: 'application']
        completed.attributes() == [:]
        inputStream.closed

        and: 'the committing path does not write local metadata sidecars'
        !metadataOperations.retrieve(key).present
        !Files.exists(metadataDirectory())
        !Files.exists(bucketPath.resolve(LocalStorageOperations.LEGACY_METADATA_DIRECTORY))
    }

    void 'byte upload failure closes the stream and does not invoke the committer'() {
        given:
        String key = 'stream-failure.txt'
        FailingInputStream inputStream = new FailingInputStream()
        UploadRequest request = UploadRequest.fromInputStream(inputStream, key, 'text/plain', null)
        AtomicInteger callbackCalls = new AtomicInteger()
        UploadCommitter<LocalStorageOperations.LocalStorageFile, String> committer = { completed ->
            callbackCalls.incrementAndGet()
            'unexpected'
        }

        when:
        operations.uploadAndCommit(request, committer)

        then:
        ObjectStorageException e = thrown()
        e.message.startsWith('Error copying file to:')
        callbackCalls.get() == 0
        inputStream.closed
        !operations.exists(key)
        !metadataOperations.retrieve(key).present
    }

    void 'failed committer removes a new object without creating metadata sidecars'() {
        given:
        String key = 'new-failure.txt'
        AtomicInteger callbackCalls = new AtomicInteger()
        UploadCommitter<LocalStorageOperations.LocalStorageFile, String> committer = { completed ->
            callbackCalls.incrementAndGet()
            throw new IllegalStateException('commit failed')
        }

        when:
        operations.uploadAndCommit(UploadRequest.fromBytes('new'.bytes, key, 'text/plain'), committer)

        then:
        IllegalStateException e = thrown()
        e.message == 'commit failed'
        callbackCalls.get() == 1
        !operations.exists(key)
        !metadataOperations.retrieve(key).present
        !Files.exists(metadataDirectory())
        snapshotFiles().empty
    }

    void 'failed replacement committer restores previous bytes and leaves application metadata unchanged'() {
        given:
        String key = 'replacement-failure.txt'
        Map<String, String> externalMetadata = new ConcurrentHashMap<>()
        operations.uploadAndCommit(UploadRequest.fromBytes('original'.bytes, key, 'text/plain'), { completed ->
            externalMetadata.put(key, 'original')
            'original-id'
        } as UploadCommitter<LocalStorageOperations.LocalStorageFile, String>)
        AtomicInteger callbackCalls = new AtomicInteger()
        UploadCommitter<LocalStorageOperations.LocalStorageFile, String> failingCommitter = { completed ->
            callbackCalls.incrementAndGet()
            throw new IllegalStateException('commit failed')
        }

        when:
        operations.uploadAndCommit(
            UploadRequest.fromBytes('replacement'.bytes, key, 'application/json'),
            failingCommitter
        )

        then:
        IllegalStateException e = thrown()
        e.message == 'commit failed'
        callbackCalls.get() == 1
        text(key) == 'original'
        externalMetadata.get(key) == 'original'
        !metadataOperations.retrieve(key).present
        snapshotFiles().empty
    }

    void 'rollback failure is suppressed on the callback failure'() {
        given:
        String key = 'rollback-failure.txt'
        UploadCommitter<LocalStorageOperations.LocalStorageFile, String> committer = { completed ->
            Path objectPath = completed.nativeResponse().get().path()
            Files.delete(objectPath)
            Files.createDirectory(objectPath)
            Files.writeString(objectPath.resolve('blocker'), 'prevent rollback')
            throw new IllegalStateException('commit failed')
        }

        when:
        operations.uploadAndCommit(UploadRequest.fromBytes('new'.bytes, key, 'text/plain'), committer)

        then:
        IllegalStateException e = thrown()
        e.message == 'commit failed'
        e.suppressed.any { it instanceof DirectoryNotEmptyException }
    }

    void 'snapshot cleanup failure after callback success does not fail or roll back replacement'() {
        given:
        String key = 'snapshot-cleanup-failure.txt'
        Map<String, String> externalMetadata = new ConcurrentHashMap<>()
        operations.uploadAndCommit(UploadRequest.fromBytes('original'.bytes, key, 'text/plain'), { completed ->
            externalMetadata.put(key, 'original')
            'original-id'
        } as UploadCommitter<LocalStorageOperations.LocalStorageFile, String>)
        UploadCommitter<LocalStorageOperations.LocalStorageFile, String> committer = { completed ->
            externalMetadata.put(key, 'replacement')
            replaceSnapshotsWithNonEmptyDirectories(snapshotDirectory())
            'replacement-id'
        }

        when:
        def committed = operations.uploadAndCommit(
            UploadRequest.fromBytes('replacement'.bytes, key, 'text/plain'),
            committer
        )

        then:
        committed.commitResult() == 'replacement-id'
        text(key) == 'replacement'
        externalMetadata.get(key) == 'replacement'
        containsNonEmptyDirectory(snapshotDirectory())
    }

    void 'committing operations for the same physical key are serialized'() {
        given:
        String key = 'serialized.txt'
        CountDownLatch firstCallbackStarted = new CountDownLatch(1)
        CountDownLatch releaseFirstCallback = new CountDownLatch(1)
        CountDownLatch secondReadStarted = new CountDownLatch(1)
        AtomicInteger callbackCalls = new AtomicInteger()
        ExecutorService executor = Executors.newFixedThreadPool(2)
        Future<?> firstUpload = null
        Future<?> secondUpload = null
        ReadStartedInputStream secondInput = new ReadStartedInputStream('second'.bytes, secondReadStarted)

        when:
        firstUpload = executor.submit({
            operations.uploadAndCommit(UploadRequest.fromBytes('first'.bytes, key, 'text/plain'), { completed ->
                callbackCalls.incrementAndGet()
                firstCallbackStarted.countDown()
                await(releaseFirstCallback)
                'first-id'
            } as UploadCommitter<LocalStorageOperations.LocalStorageFile, String>)
        } as Callable)

        then:
        firstCallbackStarted.await(5, TimeUnit.SECONDS)

        when:
        secondUpload = executor.submit({
            operations.uploadAndCommit(
                UploadRequest.fromInputStream(secondInput, key, 'text/plain', null),
                { completed ->
                    callbackCalls.incrementAndGet()
                    'second-id'
                } as UploadCommitter<LocalStorageOperations.LocalStorageFile, String>
            )
        } as Callable)

        then:
        !secondReadStarted.await(500, TimeUnit.MILLISECONDS)

        when:
        releaseFirstCallback.countDown()
        firstUpload.get(5, TimeUnit.SECONDS)
        secondUpload.get(5, TimeUnit.SECONDS)

        then:
        callbackCalls.get() == 2
        text(key) == 'second'
        secondInput.closed
        snapshotFiles().empty

        cleanup:
        releaseFirstCallback?.countDown()
        executor?.shutdownNow()
    }

    void 'concurrent committing uploads for distinct keys do not race over snapshot storage'() {
        given:
        int operationCount = 20
        int rounds = 10
        ExecutorService executor = Executors.newFixedThreadPool(operationCount)
        Map<String, String> expectedContent = [:]
        List<String> commitResults = []

        when:
        rounds.times { round ->
            CyclicBarrier startBarrier = new CyclicBarrier(operationCount)
            List<Future<String>> uploads = (0..<operationCount).collect { index ->
                String key = "concurrent-new/${round}/${index}.txt"
                String content = "new-${round}-${index}"
                expectedContent.put(key, content)
                executor.submit({
                    startBarrier.await(5, TimeUnit.SECONDS)
                    operations.uploadAndCommit(
                        UploadRequest.fromBytes(content.bytes, key, 'text/plain'),
                        { completed -> "committed-${completed.key()}" } as UploadCommitter<LocalStorageOperations.LocalStorageFile, String>
                    ).commitResult()
                } as Callable<String>)
            }
            uploads.each { upload ->
                commitResults.add(upload.get(10, TimeUnit.SECONDS))
            }
        }

        then:
        commitResults.size() == operationCount * rounds
        expectedContent.every { key, content -> text(key) == content }
        snapshotFiles().empty

        cleanup:
        executor?.shutdownNow()
    }

    void 'concurrent replacements for distinct keys do not race over snapshot storage'() {
        given:
        int operationCount = 20
        int rounds = 5
        ExecutorService executor = Executors.newFixedThreadPool(operationCount)
        List<String> keys = (0..<operationCount).collect { index -> "concurrent-replacement/${index}.txt" }
        keys.each { key ->
            operations.uploadAndCommit(
                UploadRequest.fromBytes("original-${key}".bytes, key, 'text/plain'),
                { completed -> "created-${completed.key()}" } as UploadCommitter<LocalStorageOperations.LocalStorageFile, String>
            )
        }
        Map<String, String> expectedContent = [:]

        when:
        rounds.times { round ->
            CyclicBarrier startBarrier = new CyclicBarrier(operationCount)
            List<Future<String>> replacements = keys.withIndex().collect { key, index ->
                String content = "replacement-${round}-${index}"
                expectedContent.put(key, content)
                executor.submit({
                    startBarrier.await(5, TimeUnit.SECONDS)
                    operations.uploadAndCommit(
                        UploadRequest.fromBytes(content.bytes, key, 'text/plain'),
                        { completed -> "replaced-${completed.key()}" } as UploadCommitter<LocalStorageOperations.LocalStorageFile, String>
                    ).commitResult()
                } as Callable<String>)
            }
            replacements.each { replacement ->
                replacement.get(10, TimeUnit.SECONDS)
            }
        }

        then:
        expectedContent.every { key, content -> text(key) == content }
        snapshotFiles().empty

        cleanup:
        executor?.shutdownNow()
    }

    void 'failed callback cleanup cannot remove another keys active snapshot'() {
        given:
        String failingKey = 'snapshot-isolation/failing.txt'
        String successfulKey = keyUsingDifferentMutationLock(failingKey, 'snapshot-isolation/successful')
        operations.uploadAndCommit(
            UploadRequest.fromBytes('failing-original'.bytes, failingKey, 'text/plain'),
            { completed -> 'failing-created' } as UploadCommitter<LocalStorageOperations.LocalStorageFile, String>
        )
        operations.uploadAndCommit(
            UploadRequest.fromBytes('successful-original'.bytes, successfulKey, 'text/plain'),
            { completed -> 'successful-created' } as UploadCommitter<LocalStorageOperations.LocalStorageFile, String>
        )
        CountDownLatch failingCallbackStarted = new CountDownLatch(1)
        CountDownLatch successfulCallbackStarted = new CountDownLatch(1)
        CountDownLatch allowFailure = new CountDownLatch(1)
        CountDownLatch allowSuccess = new CountDownLatch(1)
        ExecutorService executor = Executors.newFixedThreadPool(2)
        Future<?> failingUpload = executor.submit({
            operations.uploadAndCommit(
                UploadRequest.fromBytes('failing-replacement'.bytes, failingKey, 'text/plain'),
                { completed ->
                    failingCallbackStarted.countDown()
                    await(allowFailure)
                    throw new IllegalStateException('commit failed')
                } as UploadCommitter<LocalStorageOperations.LocalStorageFile, String>
            )
        } as Callable)
        Future<?> successfulUpload = executor.submit({
            operations.uploadAndCommit(
                UploadRequest.fromBytes('successful-replacement'.bytes, successfulKey, 'text/plain'),
                { completed ->
                    successfulCallbackStarted.countDown()
                    await(allowSuccess)
                    'successful-id'
                } as UploadCommitter<LocalStorageOperations.LocalStorageFile, String>
            )
        } as Callable)

        expect:
        failingCallbackStarted.await(5, TimeUnit.SECONDS)
        successfulCallbackStarted.await(5, TimeUnit.SECONDS)
        snapshotFiles().size() == 2

        when:
        allowFailure.countDown()
        failingUpload.get(5, TimeUnit.SECONDS)

        then:
        ExecutionException e = thrown()
        e.cause instanceof IllegalStateException
        e.cause.message == 'commit failed'

        and: 'the successful upload remains active with its own snapshot'
        !successfulUpload.done
        snapshotFiles().size() == 1
        text(failingKey) == 'failing-original'

        when:
        allowSuccess.countDown()
        def committed = successfulUpload.get(5, TimeUnit.SECONDS)

        then:
        committed.commitResult() == 'successful-id'
        text(successfulKey) == 'successful-replacement'
        snapshotFiles().empty

        cleanup:
        allowFailure?.countDown()
        allowSuccess?.countDown()
        executor?.shutdownNow()
    }

    private String text(String key) {
        operations.retrieve(key).get().inputStream.withCloseable { input ->
            new String(input.readAllBytes(), StandardCharsets.UTF_8)
        }
    }

    private Path metadataDirectory() {
        rootDirectory.resolve(LocalStorageOperations.INTERNAL_DIRECTORY)
            .resolve(LocalStorageLayout.METADATA_DIRECTORY)
    }

    private Path snapshotDirectory() {
        rootDirectory.resolve(LocalStorageOperations.INTERNAL_DIRECTORY)
            .resolve(LocalStorageOperations.SNAPSHOT_DIRECTORY)
            .resolve('default')
    }

    private List<Path> snapshotFiles() {
        Path directory = snapshotDirectory()
        if (!Files.exists(directory)) {
            return []
        }
        Files.walk(directory).withCloseable { stream ->
            stream.filter { path ->
                Files.isRegularFile(path) && path.fileName.toString().endsWith('.snapshot')
            }.toList()
        }
    }

    private String keyUsingDifferentMutationLock(String key, String candidatePrefix) {
        int existingIndex = LocalStorageLocks.objectMutationLockIndex(bucketPath.resolve(key))
        int suffix = 0
        String candidate
        do {
            candidate = "${candidatePrefix}-${suffix++}.txt"
        } while (LocalStorageLocks.objectMutationLockIndex(bucketPath.resolve(candidate)) == existingIndex)
        candidate
    }

    private static void replaceSnapshotsWithNonEmptyDirectories(Path snapshotDirectory) {
        List<Path> snapshots = Files.list(snapshotDirectory).withCloseable { stream ->
            stream.toList()
        }
        snapshots.each { snapshot ->
            Files.delete(snapshot)
            Files.createDirectory(snapshot)
            Files.writeString(snapshot.resolve('still-here'), 'snapshot')
        }
    }

    private static boolean containsNonEmptyDirectory(Path directory) {
        Files.list(directory).withCloseable { stream ->
            stream.anyMatch { path -> Files.isDirectory(path) && Files.exists(path.resolve('still-here')) }
        }
    }

    private static void await(CountDownLatch latch) {
        if (!latch.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException('timed out waiting for callback release')
        }
    }

    private static class CloseTrackingInputStream extends ByteArrayInputStream {
        boolean closed

        CloseTrackingInputStream(byte[] bytes) {
            super(bytes)
        }

        @Override
        void close() throws IOException {
            closed = true
            super.close()
        }
    }

    private static final class ReadStartedInputStream extends CloseTrackingInputStream {
        private final CountDownLatch readStarted

        ReadStartedInputStream(byte[] bytes, CountDownLatch readStarted) {
            super(bytes)
            this.readStarted = readStarted
        }

        @Override
        synchronized int read(byte[] bytes, int offset, int length) {
            readStarted.countDown()
            super.read(bytes, offset, length)
        }
    }

    private static final class FailingInputStream extends InputStream {
        boolean closed

        @Override
        int read() throws IOException {
            throw new IOException('stream failed')
        }

        @Override
        void close() throws IOException {
            closed = true
        }
    }
}
