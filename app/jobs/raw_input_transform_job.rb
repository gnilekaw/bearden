class RawInputTransformJob < ActiveJob::Base
  def perform(import_id)
    import = Import.find_by id: import_id
    return unless import
    raw_input = import.raw_inputs.where(state: nil).first

    finish(import) && return if raw_input.nil?

    RawInputChanges.apply raw_input
    self.class.perform_later(import_id)
  end

  def finish(import)
    SyncJob.perform_later
    import.finish
  end
end
