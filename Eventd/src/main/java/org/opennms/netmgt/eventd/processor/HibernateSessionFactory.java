package org.opennms.netmgt.eventd.processor;

import java.io.IOException;

import javax.sql.DataSource;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * @author ms043660
 *
 */
public class HibernateSessionFactory {

	private SessionFactory sessionFactory;

	private final String USER_NAME = "opennms";

	private final String SERVER_NAME = "localhost";

	private final String PASSWORD = "opennms";

	private final String DATABASE_NAME = "opennms";

	private final int portNumber = 5432;

	private final String HIBERNATE_SHOW_SQL = "hibernate.show_sql";

	private final String HIBERNATE_DIALECT = "hibernate.dialect";

	private PlatformTransactionManager platFormTransactionManager;

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public SessionFactory getSessionFactory() {
		return sessionFactory;
	}

	public HibernateSessionFactory() throws IOException, Exception {
		createSessionFactory();
	}

	public void createSessionFactory() {
		Configuration configuration = new Configuration();
		configuration.setProperty(HIBERNATE_SHOW_SQL, "true");
		configuration.setProperty(HIBERNATE_DIALECT, "org.hibernate.dialect.PostgreSQLDialect");
		sessionFactory = configuration
				.buildSessionFactory(new StandardServiceRegistryBuilder().applySettings(configuration.getProperties())
						.applySetting(Environment.DATASOURCE, getDataSource()).build());
	}

	public DataSource getDataSource() {
		PGSimpleDataSource dataSource = new PGSimpleDataSource();
		dataSource.setPortNumber(portNumber);
		dataSource.setUser(USER_NAME);
		dataSource.setPassword(PASSWORD);
		dataSource.setServerName(SERVER_NAME);
		dataSource.setDatabaseName(DATABASE_NAME);
		return dataSource;
	}

	public TransactionTemplate getTransactionTemplate() {
		final HibernateTransactionManager hibernateTransactionManager = new HibernateTransactionManager();
		hibernateTransactionManager.setSessionFactory(sessionFactory);
		platFormTransactionManager = hibernateTransactionManager;
		return new TransactionTemplate(platFormTransactionManager);
	}
}