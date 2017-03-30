# rubocop:disable Metrics/ClassLength
# rubocop:disable Metrics/MethodLength
require 'csv'

class Sync
  def self.apply
    new.apply
  end

  def initialize
    @bucket = ENV['AWS_BUCKET']
    @region = ENV['AWS_REGION']
    @file = file
  end

  def apply
    data = export
    source = upload(data)
    return unless source
    DataWarehouse.reset(source)
    sync_upstream
  end

  private

  def sync_upstream
    Redshift.connect do |conn|
      begin
        truncate(conn)
        copy(conn)
      rescue PG::Error => e
        puts e
        return errors(conn)
      end
    end
  end

  def truncate(conn)
    conn.exec("TRUNCATE #{Redshift::SCHEMA_TABLE}")
  end

  def copy(conn)
    conn.exec(
      "COPY #{Redshift::SCHEMA_TABLE} \
      (#{columns}) \
      FROM '#{@source}' \
      WITH CREDENTIALS '#{Redshift.s3_auth}' \
      DELIMITER ',' \
      REGION '#{@region}' \
      CSV IGNOREHEADER 1 EMPTYASNULL"
    )
  end

  def errors(conn)
    results = conn.exec(
      "SELECT line_number, colname, err_reason, \
        raw_field_value, raw_line \
      FROM stl_load_errors errors \
      INNER JOIN svv_table_info info \
        ON errors.tbl = info.table_id \
      WHERE filename = '#{@source}'"
    )
    results.map(&:to_a)
  end

  def columns
    CsvConverter.headers.join(',')
  end

  def export
    resolved = Organization.all.map(&OrganizationResolver.method(:resolve))
    converted = resolved.map(&CsvConverter.method(:convert))
    options = {
      headers: CsvConverter.headers,
      write_headers: true
    }
    CSV.generate(options) do |csv|
      converted.each { |row| csv << row }
    end
  end

  def upload(data)
    s3 = Aws::S3::Resource.new
    object = s3.bucket(@bucket).object(@file)
    object.put acl: 'private', body: data
    aws_location(object)
  end

  def aws_location(object)
    return nil if object.size == 0
    "s3://#{object.bucket_name}/#{object.key}"
  end

  def file
    timestamp = Time.now.strftime('%F%T').gsub(/[^0-9a-z ]/i, '')
    "reports/#{Redshift::SCHEMA_TABLE}/export_#{timestamp}.csv"
  end
end
