require 'rails_helper'

describe DataWarehouse do
  describe '.reset' do
    context 'with a valid source' do
      it 'truncates and copies the data for that source' do
        source = "s3://bucket/filename.csv"
        result = DataWarehouse.reset(source)
        expect(result.before_count).to eq 0
        expect(result.after_count).to eq 1
        # we need to ensure that we've updated the imports that were pending
      end
    end
    context 'with an invalid source' do
      it 'raises an exception' do
        expect { DataWarehouse.reset(nil) }.to raise_error(DataWarehouse::InvalidSource)
      end
    end
  end
end
