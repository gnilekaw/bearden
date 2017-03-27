require 'csv'
require 'open-uri'

class ParseCsvImportJob < ActiveJob::Base
  def perform(import_id)
    import = Import.find_by id: import_id
    return unless import

    stringio = open(import.uri)
    stringio.set_encoding Encoding::UTF_8
    data = stringio.read

    csv = CSV.parse(data, headers: true)
    csv.each { |row| import.raw_inputs.create data: row.to_h }
    RawInputTransformJob.perform_later(import.id)
  end
end
