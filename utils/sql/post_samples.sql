drop index ix_samples_eid;
create unique index ix_samples_eid ON samples USING btree (eid);

drop index ix_samples_index;
create unique index ix_samples_index ON samples USING btree (index);

vacuum analyze samples;
