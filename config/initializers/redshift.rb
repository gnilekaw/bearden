module Redshift
  SCHEMA = 'bearden_exports'
  SCHEMA_TABLE = 'bearden_exports.organizations'

  def self.connect(&block)
    conn = pg_connect
    yield conn
  ensure
    conn&.close
  end

  def self.s3_auth
    id = Rails.application.secrets.aws_access_key_id
    key = Rails.application.secrets.aws_secret_access_key
    "aws_access_key_id=#{id};aws_secret_access_key=#{key}"
  end

  private

  def self.pg_connect
    PG.connect(
      host: Rails.application.secrets.redshift_host,
      port: Rails.application.secrets.redshift_port,
      user: Rails.application.secrets.redshift_user,
      password: Rails.application.secrets.redshift_password,
      dbname: Rails.application.secrets.redshift_db
    )
  end
end
