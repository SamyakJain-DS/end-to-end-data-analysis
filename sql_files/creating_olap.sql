CREATE DATABASE OLAP;
USE OLAP;

CREATE TABLE tablets_uncleaned LIKE defaultdb.tablets_uncleaned;
CREATE TABLE laptops_uncleaned LIKE defaultdb.laptops_uncleaned;
CREATE TABLE mobiles_uncleaned LIKE defaultdb.mobiles_uncleaned;

CREATE TABLE tablets LIKE defaultdb.tablets;
CREATE TABLE laptops LIKE defaultdb.laptops;
CREATE TABLE mobiles LIKE defaultdb.mobiles;

INSERT INTO tablets_uncleaned (SELECT * FROM defaultdb.tablets_uncleaned);

INSERT INTO laptops_uncleaned (SELECT * FROM defaultdb.laptops_uncleaned);

INSERT INTO mobiles_uncleaned (SELECT * FROM defaultdb.mobiles_uncleaned);

INSERT INTO tablets (SELECT * FROM defaultdb.tablets);
INSERT INTO laptops (SELECT * FROM defaultdb.laptops);
INSERT INTO mobiles (SELECT * FROM defaultdb.mobiles);
