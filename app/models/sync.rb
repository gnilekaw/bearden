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
    @schema = 'bearden'
    @table = 'ranked'
    @schema_table = [@schema, @table].join('.')
    @file = file
    @source = "s3://#{@bucket}/#{@file}"
  end

  def apply
    upload(export)
    sync_upstream
  end

  private

  def sync_upstream
    @conn = Redshift.connect
    build_schema unless table_exists
    truncate
    copy
  rescue PG::Error => e
    puts e
    return errors
  ensure
    @conn&.close
  end

  def truncate
    @conn.exec("TRUNCATE #{@schema_table}")
  end

  def table_exists
    query = @conn.exec(
      "SELECT EXISTS ( \
        SELECT 1 \
        FROM pg_tables \
        WHERE schemaname = '#{@schema}' \
        AND tablename = '#{@table}' \
      ) AS exists"
    )
    query.first['exists'] == 't'
  end

  def build_schema
    @conn.exec("CREATE SCHEMA IF NOT EXISTS #{@schema}")
    @conn.exec(
      "CREATE TABLE IF NOT EXISTS #{@schema_table} ( \
        bearden_id integer, \
        email character varying, \
        latitude double precision, \
        longitude double precision, \
        location character varying, \
        organization_name character varying, \
        phone_number character varying, \
        tag_names character varying, \
        website character varying \
      )"
    )
  end

  def copy
    @conn.exec(
      "COPY #{@schema_table} \
      (#{columns}) \
      FROM '#{@source}' \
      WITH CREDENTIALS '#{Redshift.s3_auth}' \
      DELIMITER ',' \
      REGION '#{@region}' \
      CSV IGNOREHEADER 1 EMPTYASNULL"
    )
  end

  def errors
    results = @conn.exec(
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
  end

  def file
    timestamp = Time.now.strftime('%F%T').gsub(/[^0-9a-z ]/i, '')
    "reports/#{@schema_table}/export_#{timestamp}.csv"
  end
end
