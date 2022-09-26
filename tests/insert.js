import postgres from "postgres";
import createJsonQuerier from "../src/index.js";
import { noopLog, pipe } from "../src/utils.js";

const host = "postgresql://postgres:postgres@192.168.1.56:49155/postgres";
const sql = postgres(host, { debug: console.log });
const { insert } = await createJsonQuerier(sql);

pipe(
  insert,
  noopLog
)({
  table: "table_1",
  on_conflict: {
    constraint: "table_1_text_key",
    update_columns: ["text"],
  },
  data: [...Array(100).keys()].map((d) => ({
    text: d,
    created_time: new Date(),
    table_3: {
      key: "table_3_id",
      table: "table_3",
      on_conflict: {
        update_columns: ["value"],
        constraint: "table_3_value_key",
      },
      data: {
        value: 10 + d,
      },
    },
    table_join: {
      table: "table_join",
      parent_pkey: "id",
      key: "table_1_id",
      on_conflict: {
        update_columns: ["table_1_id", "table_2_id"],
        constraint: "table_join_table_1_id_table_2_id_key",
      },
      data: [...Array(10).keys()].map((i) => ({
        label: i + "-lol-" + d,
        table2: {
          key: "table_2_id",
          table: "table_2",
          on_conflict: {
            update_columns: ["metric"],
            constraint: "table_2_metric_key",
          },
          data: {
            metric: i * 10,
          },
        },
      })),
    },
  })),
});
