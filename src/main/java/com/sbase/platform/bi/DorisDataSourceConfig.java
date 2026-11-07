package com.sbase.platform.bi;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import javax.sql.DataSource;
@Configuration
public class DorisDataSourceConfig {
 @Bean(name="dorisDataSource") 
 DataSource dorisDataSource(@Value("${sbase.doris.jdbc-url}") String url,@Value("${sbase.doris.username}") String user,@Value("${sbase.doris.password}") String pass,@Value("${sbase.doris.driver-class-name}") String driver){
  var ds=new DriverManagerDataSource();ds.setDriverClassName(driver);ds.setUrl(url);ds.setUsername(user);ds.setPassword(pass);return ds;
 }
 @Bean(name="dorisJdbcTemplate") JdbcTemplate dorisJdbcTemplate(@Qualifier("dorisDataSource") DataSource ds){return new JdbcTemplate(ds);}
}
