select * into bad_samples_relatedness_2nd_higher_and_high_missrate from (select distinct eid from (select distinct on (id1, id2) eid
    from relatedness inner join samplesqc on (id1 = eid or id2 = eid)
    where kinship >= 0.0883
    order by id1, id2, sample_qc_missing_rate desc) t1) t2;

alter table bad_samples_relatedness_2nd_higher_and_high_missrate add primary key (eid);
