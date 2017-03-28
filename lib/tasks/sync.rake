require 'csv'

desc 'Export csv file of organizations'
task sync: :environment do
  SyncJob.perform_later
end
