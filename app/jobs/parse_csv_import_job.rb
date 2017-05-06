require 'csv'
require 'charlock_holmes/string'

class ParseCsvImportJob < ApplicationJob
  queue_as :default

  def perform(import_id)
    @blacklist = []
    @import_id = import_id
    @import = Import.find_by id: @import_id
    @data = fetch_data
    create_raw_inputs
    @import.transform
  end

  private

  def fetch_data
    response = Faraday.get @import.file_identifier.url
    raw_data = response.body

    CharlockHolmes::Converter.convert(
      raw_data,
      raw_data.detect_encoding[:encoding],
      Encoding::UTF_8.to_s
    )
  end

  def create_raw_inputs
    CSV.parse(@data, headers: true) do |row|
      raw_input = RawInput.create data: row.to_h, import_id: @import_id
      @website = row['website']
      next unless resolve_website?
      ResolveWebsiteJob.perform_later(@website, raw_input.id)
      @blacklist << @website
    end
  end

  def resolve_website?
    website_exists = Website.find_by content: @website
    blacklisted = @blacklist.include?(@website)

    !website_exists && !blacklisted
  end
end
