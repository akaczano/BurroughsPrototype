package edu.purdue.datamine.burroughsserver;

import com.viasat.burroughs.client.App;
import com.viasat.burroughs.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import com.viasat.burroughs.Burroughs;

@SpringBootApplication
public class BurroughsServerApplication {

	public static void main(String[] args) {
		SpringApplication.run(BurroughsServerApplication.class, args);
	}

	@Bean
	public ListLogger loadLogger(){
		ListLogger logger = new ListLogger();
		Logger.setLogger(logger);
		return logger;
	}

	@Bean
	public Burroughs loadBurroughs() {
		Burroughs burroughs = new Burroughs();
		App.loadConfiguration(burroughs);
		burroughs.init();
		return burroughs;
	}

	@Bean
	@Autowired
	public ConnectionHolder getConnection(Burroughs burroughs) {
		ConnectionHolder holder = new ConnectionHolder(burroughs);
		if (burroughs.connection().isDbConnected()) {
			holder.init();
		}
		return holder;
	}
}
