package com.oasisdigital.nges.event.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import javax.sql.DataSource;

import org.postgresql.ds.PGPoolingDataSource;
import org.testng.annotations.BeforeClass;

abstract public class BaseITest {
    protected static DataSource dataSource;

    @BeforeClass(alwaysRun = true)
    static public void setUpDataSource() throws Exception {
        File propertiesFile = new File("config/application.properties");
        if (!propertiesFile.exists()) {
            throw new IllegalStateException(
                    "Missing configuration file, please create config/application.properties based on sample.");
        }
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(propertiesFile)) {
            props.load(fis);
        }

        PGPoolingDataSource dataSource = new PGPoolingDataSource();
        dataSource.setServerName(props.getProperty("db.server"));
        dataSource.setDatabaseName(props.getProperty("db.database"));
        dataSource.setUser(props.getProperty("db.user"));
        dataSource.setPassword(props.getProperty("db.password"));
        dataSource.setPortNumber(Integer.parseInt(props.getProperty("db.port")));
        BaseITest.dataSource = dataSource;
    }
}
