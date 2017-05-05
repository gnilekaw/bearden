web: bundle exec puma
worker_default: bundle exec sidekiq -c ${SIDEKIQ_CONCURRENCY} -q default
worker_organization_export: bundle exec sidekiq -c ${SIDEKIQ_CONCURRENCY} -q organization_export
worker_parse_csv_import: bundle exec sidekiq -c ${SIDEKIQ_CONCURRENCY} -q parse_csv_import
worker_raw_input_transform: bundle exec sidekiq -c ${SIDEKIQ_CONCURRENCY} -q raw_input_transform
