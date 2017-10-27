drop index ix_bgen_samples_eid;
create unique index ix_bgen_samples_eid ON bgen_samples USING btree (eid);

drop index ix_bgen_samples_index;
create unique index ix_bgen_samples_index ON bgen_samples USING btree (index);

vacuum analyze bgen_samples;
