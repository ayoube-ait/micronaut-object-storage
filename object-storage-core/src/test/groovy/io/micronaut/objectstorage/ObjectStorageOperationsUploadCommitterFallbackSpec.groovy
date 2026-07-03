package io.micronaut.objectstorage

import io.micronaut.objectstorage.request.UploadRequest
import io.micronaut.objectstorage.response.UploadResponse
import spock.lang.Specification
import spock.lang.Subject

import java.util.Optional
import java.util.function.Consumer

class ObjectStorageOperationsUploadCommitterFallbackSpec extends Specification {

    @Subject
    ObjectStorageOperations<Object, Object, Object> operations = new LegacyOnlyOperations()

    void 'transactional upload completion is unsupported unless a provider opts in'() {
        given:
        UploadCommitter<Object, String> committer = { completed -> 'committed' }

        when:
        operations.uploadAndCommit(UploadRequest.fromBytes('content'.bytes, 'object.txt'), committer)

        then:
        UnsupportedOperationException e = thrown()
        e.message == 'Transactional upload completion is not supported by this provider'
    }

    private static final class LegacyOnlyOperations implements ObjectStorageOperations<Object, Object, Object> {

        @Override
        UploadResponse<Object> upload(UploadRequest request) {
            throw new UnsupportedOperationException()
        }

        @Override
        UploadResponse<Object> upload(UploadRequest request, Consumer<Object> requestConsumer) {
            throw new UnsupportedOperationException()
        }

        @Override
        Optional<ObjectStorageEntry<?>> retrieve(String key) {
            throw new UnsupportedOperationException()
        }

        @Override
        Object delete(String key) {
            throw new UnsupportedOperationException()
        }
    }
}
