class SyncJob < ApplicationJob
  def perform
    Sync.apply
  end
end
