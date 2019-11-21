package org.demo.ks;

import org.demo.ks.app.WordCountApp;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WordCountApplication implements CommandLineRunner {

  public static void main(String[] args) {
    SpringApplication.run(WordCountApplication.class, args);
  }

  @Override
  public void run(String... args) throws Exception {
    WordCountApp.run();
  }



}
