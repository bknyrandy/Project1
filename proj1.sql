show databases;

CREATE DATABASE project1;

use project1;

show tables;

-- crates all intital tables
create table if not exists BBA (drink STRING, branch STRING) row format delimited fields terminated by ',' stored as textfile;
create table if not exists BBB (drink STRING, branch STRING) row format delimited fields terminated by ',' stored as textfile;
create table if not exists BBC (drink STRING, branch STRING) row format delimited fields terminated by ',' stored as textfile;
create table if not exists BCA (drink STRING, amount INT) row format delimited fields terminated by ',' stored as textfile;
create table if not exists BCB (drink STRING, amount INT) row format delimited fields terminated by ',' stored as textfile;
create table if not exists BCC (drink STRING, amount INT) row format delimited fields terminated by ',' stored as textfile;

--loads data into the tables
LOAD DATA INPATH '/user/randy/proj1/Bev_BranchA.txt' OVERWRITE INTO TABLE BBA;
LOAD DATA INPATH '/user/randy/proj1/Bev_BranchB.txt' OVERWRITE INTO TABLE BBB;
LOAD DATA INPATH '/user/randy/proj1/Bev_BranchC.txt' OVERWRITE INTO TABLE BBC;
LOAD DATA INPATH '/user/randy/proj1/Bev_ConscountA.txt' OVERWRITE INTO TABLE BCA;
LOAD DATA INPATH '/user/randy/proj1/Bev_ConscountB.txt' OVERWRITE INTO TABLE BCB;
LOAD DATA INPATH '/user/randy/proj1/Bev_ConscountC.txt' OVERWRITE INTO TABLE BCC;


--Problem Scenario 1

-- creates table for bracnh1 
create table if not exists Branch1 as select * from bba where branch = 'Branch1'
UNION ALL 
select * from bbb where branch = 'Branch1'  --pointless since no branch1 in bbb and bbc
UNION ALL
select * from bbc where branch = 'Branch1';


--create a drinks with amount table for branch1
create table if not exists Branch1drinksT as select a.amount , a.drink from bca a join Branch1 b1 on (b1.drink = a.drink)
union ALL-- retrives only distnct records
select b.amount , b.drink from bcb b join Branch1 b1 on (b1.drink = b.drink)
UNION ALL
select c.amount , c.drink from bcc c join Branch1 b1 on (b1.drink = c.drink);


--What is the total number of consumers for Branch1?
select sum(bal.amount) as Consumers from Branch1drinksT bal;

----What is the most consumed beverage on Branch1
select drink, Max(amount) as most from branch1drinkst group by drink order by most desc limit 1;

-------------------------------------------------------------------------------------------------------------------------------------
--Problem Scenario 2

-- creates a table for bracnh2
create table if not exists Branch2 as select * from bba where branch = 'Branch2'
UNION ALL 
select * from bbb where branch = 'Branch2' 
UNION ALL
select * from bbc where branch = 'Branch2';


--create a drinks with amount table for branch2
create table if not exists Branch2drinks as select a.amount , a.drink from bca a join Branch2 b2 on (b2.drink = a.drink)
union ALL
select b.amount , b.drink from bcb b join Branch2 b2 on (b2.drink = b.drink)
UNION ALL
select c.amount , c.drink from bcc c join Branch2 b2 on (b2.drink = c.drink);


--What is the number of consumers for the Branch2?
select sum(ba2.amount) as Consumers from Branch2drinks ba2;

--What is the least consumed beverage on Branch2
select drink, sum(amount) as most from branch2drinks group by drink order by most ASC limit 1 ;

-------------------------------------------------------------------------------------------------------------------------------------
--Problem Scenario 3
-- creates table for bracnh1 
create table if not exists Branch10 as select * from bba where branch = 'Branch10'
UNION ALL 
select * from bbb where branch = 'Branch10'  
UNION ALL
select * from bbc where branch = 'Branch10';

create table if not exists Branch8 as select * from bba where branch = 'Branch8'
UNION ALL 
select * from bbb where branch = 'Branch8'  
UNION ALL
select * from bbc where branch = 'Branch8';

----What are the beverages available on Branch10, Branch8, and Branch1?
create table if not exists branch8n1 as select DISTINCT * from branch1 
union all
select DISTINCT * from branch8;


--used to display results of 3.1
select * from branch8and1 order by drink limit 20;





-- creates a table for bracnh4
create table if not exists Branch4 as select * from bba where branch = 'Branch4'
UNION ALL 
select * from bbb where branch = 'Branch4'  
UNION ALL
select * from bbc where branch = 'Branch4';

-- creates a table for bracnh7
create table if not exists Branch7 as select * from bba where branch = 'Branch7'
UNION ALL 
select * from bbb where branch = 'Branch7'  
UNION ALL
select * from bbc where branch = 'Branch7';


--create a drinks with amount table for branch4
create table if not exists Branch4drinks as select a.amount , a.drink from bca a join Branch4 b4 on (b4.drink = a.drink)
union ALL
select b.amount , b.drink from bcb b join Branch4 b4 on (b4.drink = b.drink)
UNION ALL
select c.amount , c.drink from bcc c join Branch4 b4 on (b4.drink = c.drink);


--create a drinks with amount table for branch7
create table if not exists Branch7drinks as select a.amount , a.drink from bca a join Branch7 b7 on (b7.drink = a.drink)
union ALL
select b.amount , b.drink from bcb b join Branch7 b7 on (b7.drink = b.drink)
UNION ALL
select c.amount , c.drink from bcc c join Branch7 b7 on (b7.drink = c.drink);


---common drinks between branch 4 and 7
Select distinct(b4.drink) from branch7drinks b7
join Branch4drinks b4
where b4.drink = b7.drink limit 20;

-- with intersect
select drink from branch4
intersect
select drink from branch7;


---------------------------------------------------------------------------------
--Problem Scenario 4
--create a partition,index,View for the scenario3.
--creates the partition
create table b8and1 (
drink STRING ) Partitioned by (branch STRING)
row format delimited fields terminated by ',' stored as textfile;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table b8and1 Partition(branch) select * from branch8and1;


--creates the index
create Index index_drink on table branch8and1(drink)
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
WITH DEFERRED REBUILD;

--create view
create view b8and1_view AS select * from branch8and1;


--------------------------------------------------

--5)
--Alter the table properties to add "note","comment"
ALTER TABLE branch8and1 SET TBLPROPERTIES ("comment" = "a new comment");
ALTER TABLE branch8and1 SET TBLPROPERTIES ("note" = "a new note");
--or 
ALTER TABLE branch8and1 SET TBLPROPERTIES ("comment_2" = "a new comment 2", "note_2" = "what is a new note");
--view the table properties to see the note and comment
SHOW TBLPROPERTIES branch8and1;



------------------------------
--Problem Scenario 6
--Remove the row 5 from the output of Scenario 1 


--adds a column that will keep track of data like an id
alter table branch8and1 ADD COLUMNS (row_num INTEGER);



-- makes a table with same schema as orginal table
create table b8and1_tmp like branch8and1;


--inserts data including id num into the temp table
insert into b8and1_tmp
select drink, branch, ROW_NUMBER() OVER() as row_num
from branch8and1;


--this will override the orginal table with data from the temp table
insert overwrite table branch8and1
select *
from  b8and1_tmp t
where not exists (select 1 
				from  branch8and1 tmp
				where (t.drink == tmp.drink)
						and t.row_num = 5);

