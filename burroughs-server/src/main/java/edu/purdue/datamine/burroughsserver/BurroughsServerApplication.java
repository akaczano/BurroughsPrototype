package edu.purdue.datamine.burroughsserver;

import com.viasat.burroughs.App;
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
	public Burroughs loadBurroughs() {
		Burroughs burroughs = new Burroughs();
		App.loadConfiguration(burroughs);
		burroughs.init();
		return burroughs;
	}


}
