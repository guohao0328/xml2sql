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
package org.guohao.mapping;

import java.util.ArrayList;
import java.util.List;
import org.guohao.Configuration;
import org.guohao.scripting.LanguageDriver;

/**
 * @author Clinton Begin
 */
public final class MappedStatement {

  private String resource;
  private Configuration configuration;
  private String id;
  private Integer fetchSize;
  private Integer timeout;
  private StatementType statementType;
  private SqlSource sqlSource;
  private ParameterMap parameterMap;
  private SqlCommandType sqlCommandType;
  private boolean hasNestedResultMaps;
  private LanguageDriver lang;

  MappedStatement() {
    // constructor disabled
  }

  public static class Builder {
    private MappedStatement mappedStatement = new MappedStatement();

    public Builder(Configuration configuration, String id, SqlSource sqlSource, SqlCommandType sqlCommandType) {
      mappedStatement.configuration = configuration;
      mappedStatement.id = id;
      mappedStatement.sqlSource = sqlSource;
      mappedStatement.statementType = StatementType.PREPARED;
      mappedStatement.parameterMap = new ParameterMap.Builder(configuration, "defaultParameterMap", null, new ArrayList<>()).build();
      mappedStatement.sqlCommandType = sqlCommandType;
      mappedStatement.lang = configuration.getDefaultScriptingLanguageInstance();
    }

    public Builder resource(String resource) {
      mappedStatement.resource = resource;
      return this;
    }

    public String id() {
      return mappedStatement.id;
    }

    public Builder parameterMap(ParameterMap parameterMap) {
      mappedStatement.parameterMap = parameterMap;
      return this;
    }

    public Builder fetchSize(Integer fetchSize) {
      mappedStatement.fetchSize = fetchSize;
      return this;
    }

    public Builder timeout(Integer timeout) {
      mappedStatement.timeout = timeout;
      return this;
    }

    public Builder statementType(StatementType statementType) {
      mappedStatement.statementType = statementType;
      return this;
    }

    public Builder lang(LanguageDriver driver) {
      mappedStatement.lang = driver;
      return this;
    }

    public MappedStatement build() {
      assert mappedStatement.configuration != null;
      assert mappedStatement.id != null;
      assert mappedStatement.sqlSource != null;
      assert mappedStatement.lang != null;
      return mappedStatement;
    }
  }

  public SqlCommandType getSqlCommandType() {
    return sqlCommandType;
  }

  public String getResource() {
    return resource;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public String getId() {
    return id;
  }

  public boolean hasNestedResultMaps() {
    return hasNestedResultMaps;
  }

  public Integer getFetchSize() {
    return fetchSize;
  }

  public Integer getTimeout() {
    return timeout;
  }

  public StatementType getStatementType() {
    return statementType;
  }

  public SqlSource getSqlSource() {
    return sqlSource;
  }

  public ParameterMap getParameterMap() {
    return parameterMap;
  }


  public LanguageDriver getLang() {
    return lang;
  }


  public BoundSql getBoundSql(Object parameterObject) {
    BoundSql boundSql = sqlSource.getBoundSql(parameterObject);
    List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
    if (parameterMappings == null || parameterMappings.isEmpty()) {
      boundSql = new BoundSql(configuration, boundSql.getSql(), parameterMap.getParameterMappings(), parameterObject);
    }
    return boundSql;
  }

  private static String[] delimitedStringToArray(String in) {
    if (in == null || in.trim().length() == 0) {
      return null;
    } else {
      return in.split(",");
    }
  }

}
