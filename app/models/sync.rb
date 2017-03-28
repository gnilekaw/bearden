# rubocop:disable Metrics/MethodLength
require 'csv'

class Sync
  def self.apply
    new.apply
  end

  def initialize
    @schema = 'bearden'
    @table = 'ranked'
    @schema_table = [@schema, @table].join('.')
    @file = file
    @source = "s3://#{ENV['AWS_BUCKET']}/#{@file}"
  end

  def apply
    export_csv
    sync_upstream
  end

  private

  def sync_upstream
    connection = Redshift.connect
    build_schema(connection) unless table_exists(connection)
    connection.transaction do |conn|
      truncate(conn)
      copy(conn)
    end
  rescue PG::Error
    return errors(connection)
  ensure
    connection&.close
  end

  def truncate(conn)
    conn.exec("TRUNCATE #{@schema_table}")
  end

  def table_exists(conn)
    query = conn.exec(
      "SELECT EXISTS ( \
        SELECT 1 \
        FROM pg_tables \
        WHERE schemaname = '#{@schema}' \
        AND tablename = '#{@table}' \
      ) AS exists"
    )
    query.first['exists'] == 't'
  end

  def build_schema(conn)
    conn.exec("CREATE SCHEMA IF NOT EXISTS #{@schema}")
    conn.exec(
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

  def copy(conn)
    conn.exec(
      "COPY #{@schema_table} \
      (#{columns})
      FROM '#{@source}' \
      WITH CREDENTIALS '#{Redshift.s3_auth}' \
      DELIMITER ',' \
      REGION '#{ENV['AWS_REGION']}' \
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

  # rubocop:disable Metrics/AbcSize
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

  def file
    timestamp = Time.now.strftime('%F%T').gsub(/[^0-9a-z ]/i, '')
    "reports/#{@schema_table}/export_#{timestamp}.csv"
  end
end
