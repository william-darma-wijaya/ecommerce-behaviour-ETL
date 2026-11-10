"""Copy the simulation slice of the Kaggle data into schema `source`, as text (run once)."""
from simulator import clock


def prepare(cfg):
    raw, start, end = cfg["paths"]["raw_dir"], cfg["simulation"]["start"], cfg["simulation"]["end"]
    con = clock.connect(cfg)

    con.execute(f"""
        CREATE OR REPLACE TABLE source.events AS
        SELECT *
        FROM read_csv('{raw}/events.csv', all_varchar = true)
        WHERE epoch_ms(timestamp::BIGINT) > TIMESTAMP '{start}'
          AND epoch_ms(timestamp::BIGINT) <= TIMESTAMP '{end}'
    """).fetchall()

    con.execute(f"""
        CREATE OR REPLACE TABLE source.item_properties AS
        SELECT *
        FROM read_csv('{raw}/item_properties_part*.csv', all_varchar = true)
        WHERE epoch_ms(timestamp::BIGINT) < TIMESTAMP '{end}'   -- all items: the catalogue changes even without clicks
    """).fetchall()

    con.execute(f"""
        CREATE OR REPLACE TABLE source.category_tree AS
        SELECT *
        FROM read_csv('{raw}/category_tree.csv', all_varchar = true)
    """).fetchall()

    for table in ["events", "item_properties", "category_tree"]:
        print(table, con.execute(f"SELECT count(*) FROM source.{table}").fetchall()[0][0])
    con.close()


if __name__ == "__main__":
    prepare(clock.load_config())
