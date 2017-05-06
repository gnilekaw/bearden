class AddWebsiteToRawInput < ActiveRecord::Migration[5.0]
  def change
    add_column :raw_inputs, :resolved_website, :json
  end
end
