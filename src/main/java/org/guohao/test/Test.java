package org.guohao.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import org.guohao.Configuration;
import org.guohao.builder.xml.XMLMapperBuilder;
import org.guohao.io.Resources;
import org.guohao.mapping.BoundSql;
import org.guohao.mapping.MappedStatement;

public class Test {

  public static void main(String[] args) throws IOException {
    File file = new File(
        "F:\\Develop\\projects\\github\\xml2sql\\src\\main\\java\\org\\guohao\\test\\test.xml");
    FileInputStream resourceAsStream = new FileInputStream(file);
    Configuration configuration = new Configuration();
//    InputStream resourceAsStream = Resources.getResourceAsStream("test.xml");
    XMLMapperBuilder xPathParser = new XMLMapperBuilder(resourceAsStream, configuration,
        "F:\\Develop\\projects\\github\\xml2sql\\src\\main\\java\\org\\guohao\\test\\test.xml",
        new HashMap<>());
    xPathParser.parse();
    MappedStatement findUserByName = configuration.getMappedStatement("getAllUsers");
    User user = new User();
    user.setName("ssss");
    BoundSql aaa = findUserByName.getBoundSql(user);
    String sql = aaa.getSql();
    System.out.println(sql);
  }

}
