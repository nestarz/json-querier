import {
  stringifyValueForSQL,
  pipe,
  unique,
  groupByArray,
  toArray,
  mapAsync,
} from "./utils.js";

export default (sql) => {
  const noopSql = Object.assign(sql``, { noopSql: true });
  const get = (d) => (!d || d?.noopSql === true ? null : d);
  const joinSql = (sep) => (a, b) =>
    sql`${get(a) ?? sql``}${get(a) && get(b) ? sep : sql``}${get(b) ?? sql``}`;

  const escape = (arr) =>
    arr
      .map((v) =>
        v instanceof Date
          ? `'${v.toISOString()}'::timestamp`
          : stringifyValueForSQL(v)
      )
      .map((v) => sql.unsafe(v));

  const joinDataSql = (data) =>
    data
      .map((d) => escape(d))
      .map((d) => sql`(${d.reduce(joinSql(sql`, `), noopSql)})`)
      .reduce(joinSql(sql`, `), noopSql);

  const WITH = ({ table, values }) =>
    sql`${sql(`values_${table}`)} AS (
    VALUES ${joinDataSql(values)}
  )`;

  const CONFLICT = ({ constraint, update_columns = [] }) =>
    !constraint
      ? sql``
      : sql`ON CONFLICT ON CONSTRAINT ${sql(constraint)} DO ${
          update_columns.length === 0
            ? sql`NOTHING`
            : sql`
    UPDATE SET ${update_columns
      .map((d) => sql`${sql(d)} = EXCLUDED.${sql(d)}`)
      .reduce(joinSql(sql`, `), null)}`
        }`;

  const GET_CONSTRAINT_KEYS = (constraint, table, schema = "public") =>
    sql`
  SELECT
    con.conname "constraint",
    concat(nsp.nspname, '.', rel.relname) "table",
    (
      SELECT
        array_agg(att.attname)
      FROM
        pg_attribute att
        INNER JOIN unnest(con.conkey)
        unnest(conkey) ON unnest.conkey = att.attnum
      WHERE
        att.attrelid = con.conrelid) "columns"
  FROM
    pg_constraint con
    INNER JOIN pg_class rel ON rel.oid = con.conrelid
    INNER JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
  WHERE
    nsp.nspname = ${schema}
    AND rel.relname = ${table}
    AND con.conname = ${constraint};
  
`.then((arr) => arr.flatMap((d) => d.columns));

  const INSERT_CTE = (arr) =>
    arr.filter(unique(({ table }) => table)).map(
      ({
        table,
        keys,
        foreign_keys = [],
        unique_keys = [],
        on_conflict,
      }) => sql`${sql(`cte_${table}`)} AS (
    INSERT INTO ${sql(table)} (${[
        ...keys,
        ...foreign_keys.map(({ key }) => key),
      ]
        .map((d) => sql(d))
        .reduce(joinSql(sql`, `), noopSql)})
        SELECT ${
          unique_keys?.length > 0
            ? sql`DISTINCT ON (${[
                ...unique_keys
                  .map((d) => keys.findIndex((v) => v === d))
                  .filter((v) => v > -1)
                  .map((i) => sql(`v.column${i + 2}`)),
                ...unique_keys
                  .map((d) => foreign_keys.findIndex(({ key }) => key === d))
                  .filter((v) => v > -1)
                  .map((i) => sql(`sq${i}.id`)),
              ].reduce(joinSql(sql`, `), noopSql)})`
            : sql``
        } ${[
        ...keys.map((_, i) => sql(`v.column${i + 2}`)),
        ...foreign_keys.map((_, i) => sql(`sq${i}.id`)),
      ].reduce(joinSql(sql`, `), noopSql)} FROM ${sql(`values_${table}`)} v
        ${foreign_keys
          .map(
            ({ table }, i) =>
              sql`LEFT JOIN ${sql(`cte_${table}_rn`)} ${sql(
                `sq${i}`
              )} ON v.column1[:array_length(${sql(
                `sq${i}.column1`
              )},1)] = ${sql(`sq${i}.column1`)} OR v.column1 = ${sql(
                `sq${i}.column1`
              )}[:array_length(v.column1,1)]`
          )
          .reduce(joinSql(sql` `), noopSql)}
    ${on_conflict ? CONFLICT(on_conflict) : sql``}
    RETURNING
      *
  ),
  ${sql(`cte_${table}_rn`)} AS (
    SELECT sq.column1, sq2.id FROM 
      (SELECT *, (DENSE_RANK() OVER (${
        unique_keys?.filter((d) => keys.findIndex((v) => v === d) > -1)
          ?.length > 0
          ? sql`ORDER BY ${[
              ...unique_keys
                .map((d) => keys.findIndex((v) => v === d))
                .filter((v) => v > -1)
                .map((i) => sql(`v.column${i + 2}`)),
            ].reduce(joinSql(sql`, `), noopSql)}`
          : sql``
      })) as row_number FROM ${sql(
        `values_${table}`
      )} v ORDER BY v.column1 ASC) sq
    LEFT JOIN 
      (SELECT *, (DENSE_RANK() OVER (${
        unique_keys?.length > 0
          ? sql`ORDER BY ${unique_keys
              .map((d) => sql(`cte.${d}`))
              .reduce(joinSql(sql`, `), noopSql)}`
          : sql``
      })) as row_number FROM ${sql(`cte_${table}`)} cte) sq2
    ON sq.row_number=sq2.row_number
   )`
    );

  const formatData = (arr) =>
    arr
      .flatMap(({ table, depth = 0, index = [], data: arr, ...obj }) =>
        toArray(arr ?? []).map((data, i) => ({
          table,
          depth,
          index,
          data,
          isArray: Array.isArray(arr),
          i,
          ...obj,
        }))
      )
      .flatMap(
        ({
          table,
          parentTable,
          depth,
          index: oldIndex,
          data,
          isArray,
          i,
          key,
          ...obj
        }) => {
          const values = Object.fromEntries(
            Object.entries(data).filter(([, v]) => !v?.table)
          );
          const foreigns = Object.values(data).filter((v) => v?.table);
          const index = depth ? [...oldIndex, i + 1] : [];

          return [
            {
              ...obj,
              table,
              index,
              values,
              depth,
              isArray,
              keys: Object.keys(values),
              values: Object.values(values),
              foreign_keys: [
                ...Object.values(foreigns)
                  .filter((v) => v?.table && !Array.isArray(v?.data))
                  .flatMap(({ key, table }) => ({ key, table })),
                ...(isArray ? [{ table: parentTable, key }] : []),
              ].filter((v) => v.table && v.key),
            },
            ...formatData(
              foreigns.map((obj) => ({
                ...obj,
                parentTable: table,
                index,
                depth: depth + 1,
              }))
            ),
          ];
        }
      )
      .sort(
        (a, b) =>
          (b.isArray ? -1 : 1) * b.depth - (a.isArray ? -1 : 1) * a.depth
      )
      .filter((r) => r.table !== "root");

  const addUniqueKeys = (arr) => {
    const cache = {};
    const getOrSet = async (key, set) =>
      cache[key] ?? (cache[key] = await set());
    return arr.reduce(
      ...mapAsync(async (obj) => ({
        ...obj,
        unique_keys:
          obj.on_conflict.unique_keys ??
          (await getOrSet(
            [obj.on_conflict.constraint, obj.table, obj.schema].join(";"),
            () =>
              GET_CONSTRAINT_KEYS(
                obj.on_conflict.constraint,
                obj.table,
                obj.schema
              )
          )),
      }))
    );
  };

  return pipe(
    (root) => [{ table: "root", data: { root } }],
    formatData,
    addUniqueKeys,
    (arr) =>
      arr.length > 0 &&
      sql`
    WITH ${[
      ...groupByArray(arr, ({ table }) => table)
        .map(([table, arr]) => ({
          table,
          values: arr.map((d) => [d.index, ...d.values]),
        }))
        .map(WITH),
      ...INSERT_CTE(arr),
    ].reduce(joinSql(sql`, `), noopSql)}
    SELECT 0;
  `
  );
};
