package org.octri.csvhpo;

import org.octri.csvhpo.service.DatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@Configurable
public class AppConfig {
	
	@Autowired
	JdbcTemplate jdbcTemplate;
	
	@Autowired
	PlatformTransactionManager transactionManager;
	
	@Bean
	DatabaseService databaseService() {
		return new DatabaseService(jdbcTemplate, transactionManager);
	}

}
