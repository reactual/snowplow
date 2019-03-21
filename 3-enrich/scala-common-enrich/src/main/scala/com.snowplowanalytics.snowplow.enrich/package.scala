/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import com.github.fge.jsonschema.core.report.ProcessingMessage

import common.adapters.RawEvent
import common.enrichments.registry.Enrichment

/** Scala package object to hold types, helper methods etc. */
package object common {

  /** Type alias for HTTP headers */
  type HttpHeaders = List[String]

  /** Type alias for a map whose keys are enrichment names and whose values are enrichments */
  type EnrichmentMap = Map[String, Enrichment]

  /**
   * Type alias for either a `ValidationNel` containing `String`s for `Failure` or a `List` of
   * `RawEvent`s for `Success`.
   */
  type ValidatedRawEvents = ValidatedNel[String, NonEmptyList[RawEvent]]

  /** Parameters inside of a raw event */
  type RawEventParameters = Map[String, String]

  /**
   * Type alias for either Throwable or successful value
   * It has Monad instance unlike Validation
   */
  type EitherThrowable[+A] = Either[Throwable, A]
}
