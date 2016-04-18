-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
DROP SCHEMA IF EXISTS `mydb` ;

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `mydb` DEFAULT CHARACTER SET utf8 ;
USE `mydb` ;

-- -----------------------------------------------------
-- Table `mydb`.`Tweets`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `mydb`.`Tweets` (
  `id` BIGINT NOT NULL,
  `text` VARCHAR(200) NULL,
  `timestamp` TIMESTAMP NULL,
  `isAnswer` BIGINT NULL DEFAULT 0,
  `isRetweet` TINYINT(1) NULL DEFAULT 0,
  `author` VARCHAR(45) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `mydb`.`Hashtags`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `mydb`.`Hashtags` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `text` VARCHAR(45) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `mydb`.`Tweets_has_Hashtags`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `mydb`.`Tweets_has_Hashtags` (
  `hashtag_id` INT NOT NULL,
  `tweet_id` BIGINT NOT NULL,
  PRIMARY KEY (`hashtag_id`, `tweet_id`),
  INDEX `fk_Hashtags_has_Tweets_Tweets1_idx` (`tweet_id` ASC),
  INDEX `fk_Hashtags_has_Tweets_Hashtags1_idx` (`hashtag_id` ASC),
  CONSTRAINT `fk_Hashtags_has_Tweets_Hashtags1`
    FOREIGN KEY (`hashtag_id`)
    REFERENCES `mydb`.`Hashtags` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Hashtags_has_Tweets_Tweets1`
    FOREIGN KEY (`tweet_id`)
    REFERENCES `mydb`.`Tweets` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
