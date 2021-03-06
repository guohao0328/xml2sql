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
package org.guohao.type;

import java.io.InputStream;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * The {@link TypeHandler} for {@link Blob}/{@link InputStream} using method supported at JDBC 4.0.
 * @since 3.4.0
 * @author Kazuki Shimizu
 */
public class BlobInputStreamTypeHandler extends
    BaseTypeHandler<InputStream> {

  /**
   * Set an {@link InputStream} into {@link PreparedStatement}.
   * @see PreparedStatement#setBlob(int, InputStream)
   */
  @Override
  public void setNonNullParameter(PreparedStatement ps, int i, InputStream parameter, JdbcType jdbcType)
      throws SQLException {
    ps.setBlob(i, parameter);
  }


}
