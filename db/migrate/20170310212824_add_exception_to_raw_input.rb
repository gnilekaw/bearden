class AddExceptionToRawInput < ActiveRecord::Migration[5.0]
  def change
    add_column :raw_inputs, :exception, :string
  end
end
