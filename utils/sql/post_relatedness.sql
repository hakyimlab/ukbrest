-- this query populates a table with samples that should be discarded if 2nd and higher degree are being excluded.
-- They are one of the two related samples with highest missing rate
-- If both samples in a pair have the same missing rate, then the one with the least eid is selected

select * into bad_related_samples_2nd_higher_and_high_missrate from (
    select distinct eid from (
        select distinct on (id1, id2) eid
        from relatedness inner join samplesqc on (id1 = eid or id2 = eid)
        where ckinship_0_0 >= 0.0883
        order by id1, id2, csample_qc_missing_rate_0_0 desc, eid asc
    ) t1
) t2;

alter table bad_related_samples_2nd_higher_and_high_missrate add primary key (eid);

vacuum analyze bad_related_samples_2nd_higher_and_high_missrate;
