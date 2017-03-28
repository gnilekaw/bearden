# require 'csv'

class SyncJob < ApplicationJob
  def perform
    # TODO: Is there a place on staging that this can run?
    # abort('Sync is only available on production') unless Rails.env.production?
    Sync.apply
  end
end
