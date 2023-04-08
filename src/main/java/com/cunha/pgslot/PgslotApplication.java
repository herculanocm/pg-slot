package com.cunha.pgslot;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class PgslotApplication {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(PgslotApplication.class, args);
		receiveChangesOccursBeforStartReplication();
	}



	public static String createUrl(){
		String HOST = "postgresql-production..io";
		String PORT = "5432";
		String DATABASE = "pricing_clj";
        return "jdbc:postgresql://"+HOST+':'+PORT+'/'+DATABASE;
    }

	public static void receiveChangesOccursBeforStartReplication() throws Exception {
		String url = createUrl();
		Properties props = new Properties();
		PGProperty.USER.set(props, "app_dms");
		PGProperty.PASSWORD.set(props, "");
		PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
		PGProperty.REPLICATION.set(props, "database");
		PGProperty.PREFER_QUERY_MODE.set(props, "simple");

		Connection connection = DriverManager.getConnection(url, props);
		PGConnection rplConnection = connection.unwrap(PGConnection.class);


		PGConnection pgConnection = (PGConnection) rplConnection;
	
		LogSequenceNumber lsn = LogSequenceNumber.valueOf("4/B581280");
	
		// Statement st = connection.createStatement();
		// st.execute("insert into test_logical_table(name) values('previous value')");
		// st.execute("insert into test_logical_table(name) values('previous value')");
		// st.execute("insert into test_logical_table(name) values('previous value')");
		// st.close();
	
		PGReplicationStream stream =
				pgConnection
						.getReplicationAPI()
						.replicationStream()
						.logical()
						.withSlotName("drckoquol2sihouw_00017575_7dedd887_249c_4f39_a449_893e372a9499")
						.withStartPosition(lsn)
					//    .withSlotOption("proto_version",1)
					//    .withSlotOption("publication_names", "pub1")
					   .withSlotOption("include-xids", true)
					//    .withSlotOption("skip-empty-xacts", true)
						.withStatusInterval(10, TimeUnit.SECONDS)
						.start();
		ByteBuffer buffer;
		while(true)
		{
			buffer = stream.readPending();
			if (buffer == null) {
				TimeUnit.MILLISECONDS.sleep(10L);
				continue;
			}
	
			System.out.println( toString(buffer));
			//feedback
			stream.setAppliedLSN(stream.getLastReceiveLSN());
			stream.setFlushedLSN(stream.getLastReceiveLSN());
		}
	}

	private static String toString(ByteBuffer buffer) {
        int offset = buffer.arrayOffset();
        byte[] source = buffer.array();
        int length = source.length - offset;

        return new String(source, offset, length);
    }

}
