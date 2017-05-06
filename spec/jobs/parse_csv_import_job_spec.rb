require 'rails_helper'

describe ParseCsvImportJob do
  context 'with an encoding that needs to be converted' do
    it 'converts the encoding' do
      import = Fabricate :import
      windows_encoded_data = File.read 'spec/fixtures/windows_encoded.csv'
      res = double(:response, body: windows_encoded_data)
      expect(Faraday).to receive(:get).and_return(res)
      ParseCsvImportJob.new.perform(import.id)
      expect(import.raw_inputs.count).to eq 1
    end
  end

  context 'when there are websites to resolve' do
    it 'starts a job' do
      import = Fabricate :import

      csv = File.read 'spec/fixtures/one_complete_gallery.csv'
      res = double(:response, body: csv)
      expect(Faraday).to receive(:get).and_return(res)

      expect(ResolveWebsiteJob).to receive(:perform_later)

      ParseCsvImportJob.new.perform(import.id)

      expect(import.raw_inputs.count).to eq 1
    end
  end

  context 'when there are not websites to resolve' do
    it 'does not start a job' do
      require 'csv'
      csv = File.read 'spec/fixtures/one_complete_gallery.csv'
      website = CSV.parse(csv, headers: true)['website'][0]

      import = Fabricate :import
      Fabricate(
        :website,
        content: website,
        organization: Fabricate(:organization)
      )

      res = double(:response, body: csv)
      expect(Faraday).to receive(:get).and_return(res)

      expect(ResolveWebsiteJob).to_not receive(:perform_later)

      ParseCsvImportJob.new.perform(import.id)

      expect(Website.count).to eq 1
      expect(import.raw_inputs.count).to eq 1
      # expect(import.raw_inputs.first.website).to eq website
    end
  end
end
