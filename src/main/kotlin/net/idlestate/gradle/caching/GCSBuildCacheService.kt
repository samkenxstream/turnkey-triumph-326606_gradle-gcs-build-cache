/**
 * Copyright 2019 Thorsten Ehlers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.idlestate.gradle.caching

import com.google.cloud.storage.Bucket
import com.google.cloud.storage.StorageException
import com.google.cloud.storage.StorageOptions
import org.gradle.caching.BuildCacheEntryReader
import org.gradle.caching.BuildCacheEntryWriter
import org.gradle.caching.BuildCacheException
import org.gradle.caching.BuildCacheKey
import org.gradle.caching.BuildCacheService
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.channels.Channels
import java.time.Instant

/**
 * BuildCacheService that stores the build artifacts in Google Cloud Storage.
 * The creation time will be reset in a configurable interval to make sure
 * that artifacts still in use are not deleted.
 *
 * @author Thorsten Ehlers (thorsten.ehlers@googlemail.com) (initial creation)
 */
class GCSBuildCacheService(val bucketName: String, val refreshAfterSeconds: Long) : BuildCacheService {
    private var bucket: Bucket? = null

    companion object {
        fun mightRequireReauthentication(exc: StorageException): Boolean =
            exc.code == 400 || exc.code == 401 || exc.code == 403
    }

    init {
        initStorage()
    }

    fun initBucket(): Bucket {
        val storage = StorageOptions.newBuilder()
            .build()
            .service

        return storage.get(bucketName) ?: throw BuildCacheException("$bucketName is unavailable")
    }


    fun initStorage() {
        try {
            bucket = initBucket()
        } catch (e: IOException) {
            throw BuildCacheException("IOException when accessing Google Cloud Storage bucket '$bucketName'.", e)
        } catch (e: StorageException) {
            var advice = ""
            if (mightRequireReauthentication(e)) {
                bucket = null
                advice = ", you may need to run `gcloud auth login --update-adc`"
            }
            throw BuildCacheException("Unable to access Google Cloud Storage bucket '$bucketName' (status $e.code)$advice.", e)
        }
    }

    override fun store(key: BuildCacheKey, writer: BuildCacheEntryWriter) {
        val value = ByteArrayOutputStream()
        writer.writeTo(value)

        if (bucket == null) {
            bucket = initBucket()
        }

        try {
            bucket!!.create(key.hashCode, value.toByteArray())
        } catch (e: StorageException) {
            if (mightRequireReauthentication(e)) {
                bucket = null
            }
            throw BuildCacheException("Unable to store '${key.hashCode}' in Google Cloud Storage bucket '$bucketName'.", e)
        }
    }

    override fun load(key: BuildCacheKey, reader: BuildCacheEntryReader): Boolean {
        if (bucket == null) {
            initStorage()
        }

        try {
            val blob = bucket!!.get(key.hashCode)

            if (blob != null) {
                reader.readFrom(Channels.newInputStream(blob.reader()))

                if (refreshAfterSeconds > 0) {
                    // Update creation time so that artifacts that are still used won't be deleted.
                    val createTime = Instant.ofEpochMilli(blob.createTime)
                    if (createTime.plusSeconds(refreshAfterSeconds).isBefore(Instant.now())) {
                        bucket!!.create(key.hashCode, blob.getContent())
                    }
                }

                return true
            }
        } catch (e: StorageException) {
            if (mightRequireReauthentication(e)) {
                bucket = null
            }
            // https://github.com/googleapis/google-cloud-java/issues/3402
            if (e.message?.contains("404") == true) {
                return false
            }

            throw BuildCacheException("Unable to load '${key.hashCode}' from Google Cloud Storage bucket '$bucketName'.", e)
        }

        return false
    }

    override fun close() {
        // nothing to do
    }
}
