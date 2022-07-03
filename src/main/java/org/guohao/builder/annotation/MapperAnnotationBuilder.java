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
package org.guohao.builder.annotation;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.guohao.Configuration;
import org.guohao.annotations.Case;
import org.guohao.annotations.Delete;
import org.guohao.annotations.DeleteProvider;
import org.guohao.annotations.Insert;
import org.guohao.annotations.InsertProvider;
import org.guohao.annotations.Lang;
import org.guohao.annotations.Options;
import org.guohao.annotations.Options.FlushCachePolicy;
import org.guohao.annotations.Property;
import org.guohao.annotations.Result;
import org.guohao.annotations.Select;
import org.guohao.annotations.SelectKey;
import org.guohao.annotations.SelectProvider;
import org.guohao.annotations.TypeDiscriminator;
import org.guohao.annotations.Update;
import org.guohao.annotations.UpdateProvider;
import org.guohao.builder.BuilderException;
import org.guohao.builder.IncompleteElementException;
import org.guohao.builder.MapperBuilderAssistant;
import org.guohao.builder.xml.XMLMapperBuilder;
import org.guohao.io.Resources;
import org.guohao.mapping.Discriminator;
import org.guohao.mapping.SqlCommandType;
import org.guohao.mapping.SqlSource;
import org.guohao.mapping.StatementType;
import org.guohao.parsing.PropertyParser;
import org.guohao.scripting.LanguageDriver;
import org.guohao.type.JdbcType;
import org.guohao.type.TypeHandler;
import org.guohao.type.UnknownTypeHandler;

/**
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class MapperAnnotationBuilder {

  private static final Set<Class<? extends Annotation>> statementAnnotationTypes = Stream
      .of(Select.class, Update.class, Insert.class, Delete.class, SelectProvider.class,
          UpdateProvider.class,
          InsertProvider.class, DeleteProvider.class)
      .collect(Collectors.toSet());

  private final Configuration configuration;
  private final MapperBuilderAssistant assistant;
  private final Class<?> type;

  public MapperAnnotationBuilder(Configuration configuration, Class<?> type) {
    String resource = type.getName().replace('.', '/') + ".java (best guess)";
    this.assistant = new MapperBuilderAssistant(configuration, resource);
    this.configuration = configuration;
    this.type = type;
  }

  public void parse() {
    String resource = type.toString();
    if (!configuration.isResourceLoaded(resource)) {
      loadXmlResource();
      configuration.addLoadedResource(resource);
      assistant.setCurrentNamespace(type.getName());
      for (Method method : type.getMethods()) {
        if (!canHaveStatement(method)) {
          continue;
        }
        try {
          parseStatement(method);
        } catch (IncompleteElementException e) {
          configuration.addIncompleteMethod(new MethodResolver(this, method));
        }
      }
    }
    parsePendingMethods();
  }

  private boolean canHaveStatement(Method method) {
    // issue #237
    return !method.isBridge() && !method.isDefault();
  }

  private void parsePendingMethods() {
    Collection<MethodResolver> incompleteMethods = configuration.getIncompleteMethods();
    synchronized (incompleteMethods) {
      Iterator<MethodResolver> iter = incompleteMethods.iterator();
      while (iter.hasNext()) {
        try {
          iter.next().resolve();
          iter.remove();
        } catch (IncompleteElementException e) {
          // This method is still missing a resource
        }
      }
    }
  }

  private void loadXmlResource() {
    // Spring may not know the real resource name so we check a flag
    // to prevent loading again a resource twice
    // this flag is set at XMLMapperBuilder#bindMapperForNamespace
    if (!configuration.isResourceLoaded("namespace:" + type.getName())) {
      String xmlResource = type.getName().replace('.', '/') + ".xml";
      // #1347
      InputStream inputStream = type.getResourceAsStream("/" + xmlResource);
      if (inputStream == null) {
        // Search XML mapper that is not in the module but in the classpath.
        try {
          inputStream = Resources.getResourceAsStream(type.getClassLoader(), xmlResource);
        } catch (IOException e2) {
          // ignore, resource is not required
        }
      }
      if (inputStream != null) {
        XMLMapperBuilder xmlParser = new XMLMapperBuilder(inputStream, assistant.getConfiguration(),
            xmlResource, configuration.getSqlFragments(), type.getName());
        xmlParser.parse();
      }
    }
  }


  private Properties convertToProperties(Property[] properties) {
    if (properties.length == 0) {
      return null;
    }
    Properties props = new Properties();
    for (Property property : properties) {
      props.setProperty(property.name(),
          PropertyParser.parse(property.value(), configuration.getVariables()));
    }
    return props;
  }


  private Discriminator applyDiscriminator(String resultMapId, Class<?> resultType,
      TypeDiscriminator discriminator) {
    if (discriminator != null) {
      String column = discriminator.column();
      Class<?> javaType =
          discriminator.javaType() == void.class ? String.class : discriminator.javaType();
      JdbcType jdbcType =
          discriminator.jdbcType() == JdbcType.UNDEFINED ? null : discriminator.jdbcType();
      @SuppressWarnings("unchecked")
      Class<? extends TypeHandler<?>> typeHandler = (Class<? extends TypeHandler<?>>)
          (discriminator.typeHandler() == UnknownTypeHandler.class ? null
              : discriminator.typeHandler());
      Case[] cases = discriminator.cases();
      Map<String, String> discriminatorMap = new HashMap<>();
      for (Case c : cases) {
        String value = c.value();
        String caseResultMapId = resultMapId + "-" + value;
        discriminatorMap.put(value, caseResultMapId);
      }
      return assistant.buildDiscriminator(resultType, column, javaType, jdbcType, typeHandler,
          discriminatorMap);
    }
    return null;
  }

  void parseStatement(Method method) {
    final Class<?> parameterTypeClass = getParameterType(method);
    final LanguageDriver languageDriver = getLanguageDriver(method);

    getAnnotationWrapper(method, true, statementAnnotationTypes).ifPresent(statementAnnotation -> {
      final SqlSource sqlSource = buildSqlSource(statementAnnotation.getAnnotation(),
          parameterTypeClass, languageDriver, method);
      final SqlCommandType sqlCommandType = statementAnnotation.getSqlCommandType();
      final Options options = getAnnotationWrapper(method, false, Options.class)
          .map(x -> (Options) x.getAnnotation()).orElse(null);
      final String mappedStatementId = type.getName() + "." + method.getName();
      Integer fetchSize = null;
      Integer timeout = null;
      StatementType statementType = StatementType.PREPARED;
      boolean isSelect = sqlCommandType == SqlCommandType.SELECT;
      boolean flushCache = !isSelect;
      boolean useCache = isSelect;
      if (options != null) {
        if (FlushCachePolicy.TRUE.equals(options.flushCache())) {
          flushCache = true;
        } else if (FlushCachePolicy.FALSE.equals(options.flushCache())) {
          flushCache = false;
        }
        useCache = options.useCache();
        fetchSize = options.fetchSize() > -1 || options.fetchSize() == Integer.MIN_VALUE ? options
            .fetchSize() : null; //issue #348
        timeout = options.timeout() > -1 ? options.timeout() : null;
        statementType = options.statementType();
      }

      assistant.addMappedStatement(
          mappedStatementId,
          sqlSource,
          statementType,
          sqlCommandType,
          fetchSize,
          timeout,
          // ParameterMapID
          null,
          parameterTypeClass,
          // TODO gcode issue #577,
          languageDriver);
    });
  }

  private LanguageDriver getLanguageDriver(Method method) {
    Lang lang = method.getAnnotation(Lang.class);
    Class<? extends LanguageDriver> langClass = null;
    if (lang != null) {
      langClass = lang.value();
    }
    return configuration.getLanguageDriver(langClass);
  }

  private Class<?> getParameterType(Method method) {
    Class<?> parameterType = null;
    Class<?>[] parameterTypes = method.getParameterTypes();
    for (Class<?> currentParameterType : parameterTypes) {
      parameterType = currentParameterType;
    }
    return parameterType;
  }

  private String findColumnPrefix(Result result) {
    String columnPrefix = result.one().columnPrefix();
    if (columnPrefix.length() < 1) {
      columnPrefix = result.many().columnPrefix();
    }
    return columnPrefix;
  }

  private String nestedResultMapId(Result result) {
    String resultMapId = result.one().resultMap();
    if (resultMapId.length() < 1) {
      resultMapId = result.many().resultMap();
    }
    if (!resultMapId.contains(".")) {
      resultMapId = type.getName() + "." + resultMapId;
    }
    return resultMapId;
  }

  private boolean hasNestedResultMap(Result result) {
    if (result.one().resultMap().length() > 0 && result.many().resultMap().length() > 0) {
      throw new BuilderException("Cannot use both @One and @Many annotations in the same @Result");
    }
    return result.one().resultMap().length() > 0 || result.many().resultMap().length() > 0;
  }

  private String nestedSelectId(Result result) {
    String nestedSelect = result.one().select();
    if (nestedSelect.length() < 1) {
      nestedSelect = result.many().select();
    }
    if (!nestedSelect.contains(".")) {
      nestedSelect = type.getName() + "." + nestedSelect;
    }
    return nestedSelect;
  }

  private boolean hasNestedSelect(Result result) {
    if (result.one().select().length() > 0 && result.many().select().length() > 0) {
      throw new BuilderException("Cannot use both @One and @Many annotations in the same @Result");
    }
    return result.one().select().length() > 0 || result.many().select().length() > 0;
  }


  private String nullOrEmpty(String value) {
    return value == null || value.trim().length() == 0 ? null : value;
  }

  private SqlSource buildSqlSource(Annotation annotation, Class<?> parameterType,
      LanguageDriver languageDriver,
      Method method) {
    if (annotation instanceof Select) {
      return buildSqlSourceFromStrings(((Select) annotation).value(), parameterType,
          languageDriver);
    } else if (annotation instanceof Update) {
      return buildSqlSourceFromStrings(((Update) annotation).value(), parameterType,
          languageDriver);
    } else if (annotation instanceof Insert) {
      return buildSqlSourceFromStrings(((Insert) annotation).value(), parameterType,
          languageDriver);
    } else if (annotation instanceof Delete) {
      return buildSqlSourceFromStrings(((Delete) annotation).value(), parameterType,
          languageDriver);
    } else if (annotation instanceof SelectKey) {
      return buildSqlSourceFromStrings(((SelectKey) annotation).statement(), parameterType,
          languageDriver);
    }
    return new ProviderSqlSource(assistant.getConfiguration(), annotation, type, method);
  }

  private SqlSource buildSqlSourceFromStrings(String[] strings, Class<?> parameterTypeClass,
      LanguageDriver languageDriver) {
    return languageDriver
        .createSqlSource(configuration, String.join(" ", strings).trim(), parameterTypeClass);
  }

  @SafeVarargs
  private final Optional<AnnotationWrapper> getAnnotationWrapper(Method method,
      boolean errorIfNoMatch,
      Class<? extends Annotation>... targetTypes) {
    return getAnnotationWrapper(method, errorIfNoMatch, Arrays.asList(targetTypes));
  }

  private Optional<AnnotationWrapper> getAnnotationWrapper(Method method, boolean errorIfNoMatch,
      Collection<Class<? extends Annotation>> targetTypes) {
    Map<String, AnnotationWrapper> statementAnnotations = targetTypes.stream()
        .flatMap(x -> Arrays.stream(method.getAnnotationsByType(x))).map(AnnotationWrapper::new)
        .collect(
            Collectors.toMap(AnnotationWrapper::getDatabaseId, x -> x, (existing, duplicate) -> {
              throw new BuilderException(
                  String.format("Detected conflicting annotations '%s' and '%s' on '%s'.",
                      existing.getAnnotation(), duplicate.getAnnotation(),
                      method.getDeclaringClass().getName() + "." + method.getName()));
            }));
    AnnotationWrapper annotationWrapper = statementAnnotations.get("");

    if (errorIfNoMatch && annotationWrapper == null && !statementAnnotations.isEmpty()) {
      // Annotations exist, but there is no matching one for the specified databaseId
      throw new BuilderException(
          String.format(
              "Could not find a statement annotation that correspond a current database or default statement on method '%s.%s'. Current database id is [%s].",
              method.getDeclaringClass().getName(), method.getName()));
    }
    return Optional.ofNullable(annotationWrapper);
  }

  private class AnnotationWrapper {

    private final Annotation annotation;
    private final String databaseId;
    private final SqlCommandType sqlCommandType;

    AnnotationWrapper(Annotation annotation) {
      super();
      this.annotation = annotation;
      if (annotation instanceof Select) {
        databaseId = ((Select) annotation).databaseId();
        sqlCommandType = SqlCommandType.SELECT;
      } else if (annotation instanceof Update) {
        databaseId = ((Update) annotation).databaseId();
        sqlCommandType = SqlCommandType.UPDATE;
      } else if (annotation instanceof Insert) {
        databaseId = ((Insert) annotation).databaseId();
        sqlCommandType = SqlCommandType.INSERT;
      } else if (annotation instanceof Delete) {
        databaseId = ((Delete) annotation).databaseId();
        sqlCommandType = SqlCommandType.DELETE;
      } else if (annotation instanceof SelectProvider) {
        databaseId = ((SelectProvider) annotation).databaseId();
        sqlCommandType = SqlCommandType.SELECT;
      } else if (annotation instanceof UpdateProvider) {
        databaseId = ((UpdateProvider) annotation).databaseId();
        sqlCommandType = SqlCommandType.UPDATE;
      } else if (annotation instanceof InsertProvider) {
        databaseId = ((InsertProvider) annotation).databaseId();
        sqlCommandType = SqlCommandType.INSERT;
      } else if (annotation instanceof DeleteProvider) {
        databaseId = ((DeleteProvider) annotation).databaseId();
        sqlCommandType = SqlCommandType.DELETE;
      } else {
        sqlCommandType = SqlCommandType.UNKNOWN;
        if (annotation instanceof Options) {
          databaseId = ((Options) annotation).databaseId();
        } else if (annotation instanceof SelectKey) {
          databaseId = ((SelectKey) annotation).databaseId();
        } else {
          databaseId = "";
        }
      }
    }

    Annotation getAnnotation() {
      return annotation;
    }

    SqlCommandType getSqlCommandType() {
      return sqlCommandType;
    }

    String getDatabaseId() {
      return databaseId;
    }
  }
}
