/*
 * Copyright 2017-2026 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.objectstorage;

import io.micronaut.core.annotation.Blocking;
import io.micronaut.objectstorage.request.UploadRequest;
import org.jspecify.annotations.NonNull;

/**
 * Capability for object storage providers that can invoke an application publication callback after
 * finalizing an upload.
 *
 * <p>The callback is invoked synchronously and exactly once, after the bytes are durable. It is not invoked when the
 * byte upload fails. Providers do not retry the callback. A successful return publishes the upload; an exception
 * rejects publication and causes the provider to attempt its documented compensation.</p>
 *
 * <p>The callback must not re-enter storage operations for the same physical key. It owns application metadata and
 * transaction boundaries; the provider owns byte storage and compensation for provider-managed state.</p>
 *
 * @param <O> The provider-native upload response type.
 * @since 3.1.0
 */
public interface CommittingObjectStorageOperations<O> {

    /**
     * Uploads an object and invokes an application callback with finalized storage information.
     *
     * @param request The upload request.
     * @param committer The synchronous application publication callback.
     * @param <R> The application-defined commit result type.
     * @return The provider upload response and application commit result.
     * @throws ObjectStorageException if byte storage or provider compensation fails
     * @throws RuntimeException if the application callback fails; compensation failures are attached as suppressed
     * exceptions
     * @since 3.1.0
     */
    @Blocking
    @NonNull
    <R> CommittedUpload<O, R> uploadAndCommit(@NonNull UploadRequest request,
                                              @NonNull UploadCommitter<O, R> committer);
}
