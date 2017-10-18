package org.opennms.netmgt.eventd.processor;

import java.io.IOException;
import java.util.Properties;

import javax.sql.DataSource;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.orm.hibernate5.LocalSessionFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public class SpringConfiguration {

	public SpringConfiguration() throws IOException, Exception {
		sessionFactory(getDataSource());
	}

	public LocalSessionFactoryBean sessionFactory(final DataSource dataSource) throws IOException, Exception {
		final Properties hp = new Properties();

		final LocalSessionFactoryBean factory = new LocalSessionFactoryBean();
		factory.setDataSource(dataSource);
		factory.setPackagesToScan(new String[] { "org.opennms.model", "org.opennms.netmgt.model" });
		factory.setHibernateProperties(hp);
		hp.put("hibernate.dialect", "org.hibernate.dialect.H2Dialect");
		hp.put("hibernate.hbm2ddl.auto", "create-drop");
		return factory;
	}

	public SessionFactory getSessionFactory() {
		// create configuration using hibernate API
		Configuration configuration = new Configuration();
		configuration.setProperty("hibernate.show_sql","true");
		configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
//		configuration.setProperty("hibernate.connection.username", "opennms");
//		configuration.setProperty("hibernate.connection.password", "opennms");
		return configuration
			    .buildSessionFactory(
			        new StandardServiceRegistryBuilder()
			            .applySettings(configuration.getProperties())
			            //here you apply the custom dataSource
			            .applySetting(Environment.DATASOURCE, getDataSource())
			            .build());
		//return configuration.buildSessionFactory();
	}

	public DataSource getDataSource() {

		PGSimpleDataSource dataSource = new PGSimpleDataSource();
		dataSource.setPortNumber(5432);
		dataSource.setUser("opennms");
		dataSource.setPassword("opennms");
		dataSource.setServerName("localhost");
		dataSource.setDatabaseName("opennms");
		return dataSource;
	}

	public PlatformTransactionManager transactionManager(SessionFactory sessionFatory) {
		final HibernateTransactionManager txm = new HibernateTransactionManager();
		txm.setSessionFactory(sessionFatory);
		return txm;
	}

	public TransactionTemplate transactionTemplate(PlatformTransactionManager ptm) {
		return new TransactionTemplate(ptm);
	}

}