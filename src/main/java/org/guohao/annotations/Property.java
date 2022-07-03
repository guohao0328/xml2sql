/*
 *    Copyright 2009-2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.guohao.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The annotation that inject a property value.
 *
 * @since 3.4.2
 * @author Kazuki Shimizu
 * @see CacheNamespace
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface Property {

  /**
   * Returns the property name.
   *
   * @return the property name
   */
  String name();

  /**
   * Returns the property value or placeholder.
   *
   * @return the property value or placeholder
   */
  String value();
}
