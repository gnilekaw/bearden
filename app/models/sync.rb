require 'csv'

class Sync
  def self.apply
    new.apply
  end

  def initialize
    @file = file
    @table = 'bearden.ranked'
  end

  def apply
    export_csv

    connection = Redshift.connect
    connection.transaction do |conn|
      truncate(conn)
      copy(conn)
    end

  rescue PG::Error => e
    return e.message

  ensure
    connection&.close
  end

  private

  def truncate(conn)
    conn.exec("TRUNCATE #{@table}")
  end

  def copy(conn)
    source = "s3://#{ENV['AWS_BUCKET']}/#{@file}"
    conn.exec("COPY #{@table} \
                FROM '#{source}' \
                WITH CREDENTIALS '#{Redshift.s3_auth}' \
                DELIMITER ',' \
                REGION '#{ENV['AWS_REGION']}' \
                CSV IGNOREHEADER 1 EMPTYASNULL")
  end

  # rubocop:disable Metrics/AbcSize
  # rubocop:disable Metrics/MethodLength
  def export_csv
    resolved = Organization.all.map(&OrganizationResolver.method(:resolve))
    converted = resolved.map(&CsvConverter.method(:convert))
    options = {
      headers: CsvConverter.headers,
      write_headers: true
    }
    csv_data = CSV.generate(options) do |csv|
      converted.each { |row| csv << row }
    end
    s3 = Aws::S3::Resource.new
    object = s3.bucket(ENV['AWS_BUCKET']).object(@file)
    object.put acl: 'private', body: csv_data
  end
  # rubocop:enable Metrics/AbcSize
  # rubocop:enable Metrics/MethodLength

  def file
    timestamp = Time.now.strftime('%F%T').gsub(/[^0-9a-z ]/i, '')
    "reports/#{@table}/export_#{timestamp}.csv"
  end
end
