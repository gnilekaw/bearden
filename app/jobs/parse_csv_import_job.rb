require 'csv'
require 'open-uri'
require 'charlock_holmes/string'

class ParseCsvImportJob < ApplicationJob
  attr_accessor :import

  queue_as :parse_csv_import

  def perform(import_id)
    @import = Import.find_by id: import_id
    return unless @import

    create_raw_inputs
    @import.transform
  end

  private

  def data
    response = Faraday.get @import.file_identifier.url
    raw_data = response.body

    CharlockHolmes::Converter.convert(
      raw_data,
      raw_data.detect_encoding[:encoding],
      Encoding::UTF_8.to_s
    )
  end

  def create_raw_inputs
    csv = CSV.parse(data, headers: true)
    csv.each { |row| @import.raw_inputs.create data: row.to_h }
  end
end
