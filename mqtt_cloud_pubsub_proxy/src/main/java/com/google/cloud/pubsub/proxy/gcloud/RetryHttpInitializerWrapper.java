/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pubsub.proxy.gcloud;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;

import java.io.IOException;

/**
 * RetryHttpInitializerWrapper will automatically retry upon RPC
 * failures, preserving the auto-refresh behavior of the Google
 * Credentials.
 */
public class RetryHttpInitializerWrapper implements HttpRequestInitializer {

  /**
   * Intercepts the request for filling in the "Authorization"
   * header field, as well as recovering from certain unsuccessful
   * error codes wherein the Credential must refresh its token for a
   * retry.
   */
  private final Credential wrappedCredential;

  /**
   * A sleeper variable.
   */
  private final Sleeper sleeper;

  /**
   * How many seconds to wait before attempting retry.
   * TODO(rshanky) read from config file
   */
  private static final int RETRY_SECONDS = 120;

  /**
   * Initialize variables for retry.
   *
   * @param wrappedCredential Google Credentials for retry.
   */
  public RetryHttpInitializerWrapper(final Credential wrappedCredential) {
    this.wrappedCredential = wrappedCredential;
    this.sleeper = Sleeper.DEFAULT;
  }

  @Override
  public final void initialize(final HttpRequest request) {
    request.setReadTimeout(RETRY_SECONDS * 1000); // 2 minute timeout
    final HttpUnsuccessfulResponseHandler backoffHandler =
        new HttpBackOffUnsuccessfulResponseHandler(new ExponentialBackOff()).setSleeper(sleeper);
    request.setInterceptor(wrappedCredential);
    request.setUnsuccessfulResponseHandler(new HttpUnsuccessfulResponseHandler() {
      @Override
      public boolean handleResponse(final HttpRequest request, final HttpResponse response,
          final boolean supportsRetry) throws IOException {
        if (wrappedCredential.handleResponse(request, response, supportsRetry)) {
          // Back off not needed if credential decides it
          // can handle it or return code indicates authentication
          return true;
        } else {
          return backoffHandler.handleResponse(request, response, supportsRetry);
        }
      }
    });
    request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(new ExponentialBackOff())
        .setSleeper(sleeper));
  }
}
