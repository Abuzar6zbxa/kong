return {
  postgres = {
    up = [[
      DO $$
      BEGIN
      ALTER TABLE IF EXISTS ONLY "clustering_data_planes" ADD "cert_details" JSONB;
      EXCEPTION WHEN DUPLICATE_COLUMN THEN
        -- Do nothing, accept existing state
        --
      ALTER TABLE IF EXISTS ONLY "clustering_data_planes" ADD "version" INTEGER;
      EXCEPTION WHEN DUPLICATE_COLUMN THEN
        -- Do nothing, accept existing state
      END;

      ALTER TABLE IF EXISTS ONLY "clustering_data_planes" DROP COLUMN "config_hash";
      $$;

      DO $$
      BEGIN
      CREATE TABLE clustering_sync_version (
        "version" SERIAL PRIMARY KEY
      );

      CREATE TABLE clustering_sync_delta (
        "version" INT NOT NULL,
        "type" TEXT NOT NULL,
        "id" UUID NOT NULL,
        "ws_id" UUID NOT NULL,
        "row" JSON,
        FOREIGN KEY (version) REFERENCES clustering_sync_version(version)
      );

      CREATE INDEX clustering_sync_delta_version_idx ON clustering_sync_delta (version);

      END;
      $$;
    ]]
  }
}
