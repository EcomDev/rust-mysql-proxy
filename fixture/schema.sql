DROP DATABASE IF EXISTS catalog;
DROP DATABASE IF EXISTS another_catalog;

CREATE DATABASE catalog;
CREATE DATABASE another_catalog;

USE catalog;

CREATE TABLE `product` (
    `product_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `sku` VARCHAR(255) NOT NULL,
    `type` VARCHAR(255) NOT NULL,
    `created_at` DATETIME NOT NULL,
    PRIMARY KEY (`product_id`),
    UNIQUE KEY `sku` (`sku`)
) ENGINE=InnoDB;

CREATE TABLE `product_int` (
    `value_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `product_id` INT UNSIGNED NOT NULL,
    `attribute_id` INT UNSIGNED NOT NULL,
    `value` INT,
    PRIMARY KEY (`value_id`),
    UNIQUE KEY (`product_id`, `attribute_id`)
) ENGINE=InnoDB;

CREATE TABLE `product_decimal` (
   `value_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
   `product_id` INT UNSIGNED NOT NULL,
   `attribute_id` INT UNSIGNED NOT NULL,
   `value` DECIMAL(12,4),
   PRIMARY KEY (`value_id`),
   UNIQUE KEY (`product_id`, `attribute_id`)
) ENGINE=InnoDB;

CREATE TABLE `product_varchar` (
   `value_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
   `product_id` INT UNSIGNED NOT NULL,
   `attribute_id` INT UNSIGNED NOT NULL,
   `value` VARCHAR(255),
   PRIMARY KEY (`value_id`),
   UNIQUE KEY (`product_id`, `attribute_id`)
) ENGINE=InnoDB;

CREATE TABLE `product_text` (
    `value_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `product_id` INT UNSIGNED NOT NULL,
    `attribute_id` INT UNSIGNED NOT NULL,
    `value` TEXT,
    PRIMARY KEY (`value_id`),
    UNIQUE KEY (`product_id`, `attribute_id`)
) ENGINE=InnoDB;

CREATE TABLE `some_sequence` (
    `sequence_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (`sequence_id`)
);

DELIMITER //

CREATE PROCEDURE all_product_data(
    IN filter_sku VARCHAR(255)
)
BEGIN
    SELECT sku,type FROM product WHERE sku LIKE filter_sku;
    SELECT p.sku,pd.attribute_id,pd.value FROM product p INNER JOIN product_int pd ON pd.product_id = p.product_id WHERE p.sku LIKE filter_sku;
    SELECT p.sku,pd.attribute_id,pd.value FROM product p INNER JOIN product_decimal pd ON pd.product_id = p.product_id WHERE p.sku LIKE filter_sku;
    SELECT p.sku,pd.attribute_id,pd.value FROM product p INNER JOIN product_varchar pd ON pd.product_id = p.product_id WHERE p.sku LIKE filter_sku;
    SELECT p.sku,pd.attribute_id,pd.value FROM product p INNER JOIN product_text pd ON pd.product_id = p.product_id WHERE p.sku LIKE filter_sku;
END //

DELIMITER ;

INSERT INTO product (product_id, sku, type, created_at)
    VALUES (1, 'SKU1', 'simple', '2010-01-01 00:00:00'),
           (2, 'SKU2', 'simple', '2011-01-01 10:10:10'),
           (3, 'SKU3', 'simple', '2012-01-01 20:20:20'),
           (4, 'SKU4', 'configurable', '2013-01-01 01:10:10');

INSERT INTO product_int (product_id, attribute_id, value)
    VALUES (1, 1, 1),
           (2, 1, 1),
           (3, 1, 1),
           (4, 1, 0)
           ;

INSERT INTO product_decimal (product_id, attribute_id, value)
VALUES (1, 2, 10.00),
       (2, 2, 5.00),
       (3, 2, 5.99),
       (4, 2, 0.00)
;


INSERT INTO product_varchar (product_id, attribute_id, value)
VALUES (1, 3, 'Name 1'),
       (2, 3, 'Name 2'),
       (3, 3, 'Name 3'),
       (4, 3, 'Name 4')
;

INSERT INTO product_text (product_id, attribute_id, value)
VALUES (1, 4, 'Description 1'),
       (2, 4, 'Description 2'),
       (3, 4, 'Description 3'),
       (4, 4, 'Description 4')
;

