create table Txtdata (

songid varchar(100),
hour int,
songcount int
);

create table txtdata1 (

songid varchar(100),
songcount int);

LOAD DATA LOCAL INFILE 'C:/Users/Aishwerya Rastogi/Desktop/SaavnProjectOutput/part-00025.txt' INTO TABLE txtdata LINES TERMINATED BY '\n';
select * from txtdata

##select songid , SUM(songcount) as temp from txtdata group by songid ORDER BY temp DESC LIMIT 100;

INSERT INTO txtdata1 (songid, songcount) SELECT txtdata.songid , SUM(txtdata.songcount) from txtdata GROUP BY txtdata.songid

select * from txtdata1

select DISTINCT(songid) from txtdata1 ORDER BY songcount DESC LIMIT 100

drop table txtdata;
drop table txtdata1;





