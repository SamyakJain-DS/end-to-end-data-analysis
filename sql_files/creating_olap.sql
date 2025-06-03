CREATE DATABASE project_olap;
USE project_olap;

CREATE TABLE tablets_uncleaned LIKE project.tablets_uncleaned;
CREATE TABLE laptops_uncleaned LIKE project.laptops_uncleaned;
CREATE TABLE mobiles_uncleaned LIKE project.mobiles_uncleaned;

CREATE TABLE tablets LIKE project.tablets;
CREATE TABLE laptops LIKE project.laptops;

-- INSERT INTO tablets_uncleaned (SELECT * FROM project.tablets_uncleaned);

-- INSERT INTO laptops_uncleaned (SELECT * FROM project.laptops_uncleaned);

-- INSERT INTO mobiles_uncleaned (SELECT * FROM project.mobiles_uncleaned);

-- INSERT INTO tablets (SELECT * FROM project.tablets);
-- INSERT INTO laptops (SELECT * FROM project.laptops);
