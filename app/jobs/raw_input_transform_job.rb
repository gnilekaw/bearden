class RawInputTransformJob < ActiveJob::Base
  def perform(import_id)
    import = Import.find_by id: import_id
    return unless import
    raw_input = import.raw_inputs.where(state: nil).first

    if raw_input.nil?
      SyncJob.perform_later
      import.finish
      return
    else
      RawInputChanges.apply raw_input
      self.class.perform_later(import_id)
    end
  end
end
