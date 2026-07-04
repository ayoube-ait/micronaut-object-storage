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

import io.micronaut.objectstorage.response.UploadResponse;
import org.jspecify.annotations.NonNull;

import java.util.Objects;

/**
 * The provider upload response and the application-defined commit result.
 *
 * @param uploadResponse The provider upload response.
 * @param commitResult The application-defined commit result.
 * @param <O> The provider-native upload response type.
 * @param <R> The application-defined commit result type.
 * @since 3.1.0
 */
public record CommittedUpload<O, R>(
    @NonNull UploadResponse<O> uploadResponse,
    @NonNull R commitResult
) {
    /**
     * Creates a committed upload.
     */
    public CommittedUpload {
        uploadResponse = Objects.requireNonNull(uploadResponse, "uploadResponse");
        commitResult = Objects.requireNonNull(commitResult, "commitResult");
    }
}
