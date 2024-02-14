package com.kafka;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class Main {
  public static void main(final String[] args)
  {
      Logger logger = LoggerFactory.getLogger(Main.class);
      logger.debug("Debug Message Logged !!!");
      logger.info("Info Message Logged !!!");
      logger.error("Error Message Logged !!!", new NullPointerException("NullError"));
  }
}