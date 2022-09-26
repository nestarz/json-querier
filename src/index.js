import {
  stringifyValueForSQL,
  pipe,
  unique,
  groupByArray,
  toArray,
  mapAsync,
  createFlatten,
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
      .reduce(joinSql(sql`,\n\t`), noopSql);

  const getTypes = pipe(
    (table) => sql`
        SELECT
          column_name,
          data_type
        FROM
          information_schema.columns
        WHERE
          table_name = ${table};
        
      `,
    (arr) => arr.map(({ column_name, data_type }) => [column_name, data_type]),
    Object.fromEntries
  );

  // INSERT
  const WITH = ({ table, values }) => {
    return sql`${sql(`values_${table}_raw`)} (__id) AS (
VALUES ${joinDataSql(values)}
), ${sql(`values_${table}`)} AS (
  SELECT t.*, (ARRAY(SELECT jsonb_array_elements(d.value)))::int[] as column1 
  FROM ${sql(`values_${table}_raw`)} t, jsonb_array_elements(t.__id::jsonb) as d
)`;
  };

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
INSERT INTO ${sql(table)} (${[...keys, ...foreign_keys.map(({ key }) => key)]
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
        )} ON v.column1[:array_length(${sql(`sq${i}.column1`)},1)] = ${sql(
          `sq${i}.column1`
        )} OR v.column1 = ${sql(`sq${i}.column1`)}[:array_length(v.column1,1)]`
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

  // UPDATE
  const mapCompare = {
    _eq: sql`=`,
    _neq: sql`!=`,
    _lt: sql`<`,
    _gt: sql`>`,
    _in: sql`IN`,
    _nin: sql`NOT IN`,
    _lte: sql`<=`,
    _gte: sql`>=`,
    _contains: sql`@>`,
    _contained_in: sql`<@`,
    _has_key: sql`?`,
    _has_keys_any: sql`?|`,
    _has_keys_all: sql`?&`,
  };
  return {
    update: async ({ table, updates }) => {
      const flatten = createFlatten({ ok: (prop) => prop[0] !== "_" });
      const types = await getTypes(table);
      const keys = [
        ...new Set(updates.flatMap(({ _set }) => Object.keys(_set))),
      ].sort();
      const whereKeys = [
        ...new Set(updates.flatMap(({ where }) => Object.keys(flatten(where)))),
      ].sort();
      const data = updates
        .map((obj) => ({
          ...obj,
          keys: Object.keys(flatten(obj.where))
            .filter(unique((v) => v))
            .sort(),
          values: whereKeys.map((k) => flatten(obj.where)[k]),
        }))
        .map((v, _, arr) => ({
          ...v,
          keys,
          rank: groupByArray(arr, ({ values, keys }) =>
            JSON.stringify([keys, values])
          ).findIndex(([str]) => str === JSON.stringify([v.keys, v.values])),
        }));

      return !(updates?.length > 0)
        ? []
        : sql`WITH cte_values ("__where_rank", "__to_update", ${keys
            .map((key) => sql(key))
            .reduce(joinSql(sql`, `, noopSql))}) AS (
    VALUES ${joinDataSql(
      data.map(({ rank, _set }) => [
        rank,
        Object.keys(_set),
        ...keys.map((k) => _set[k]),
      ])
    )}
      ),\n\ncte_where ("rank", "__where_keys", ${whereKeys
        .map((key) => sql(key))
        .reduce(joinSql(sql`, `, noopSql))}) AS (
    VALUES ${joinDataSql(
      data
        .filter(unique(({ rank }) => rank))
        .map(({ values, rank, where }) => [
          rank,
          Object.keys(flatten(where)),
          ...values,
        ])
    )}
      ),\n\ncte_update AS (
      UPDATE ${sql(table)} t
      SET ${keys
        .map(
          (key) =>
            sql`${sql(key)} = (CASE WHEN ${sql.unsafe(
              stringifyValueForSQL(key)
            )} = ANY(cte_values.__to_update) THEN cte_values.${sql(
              key
            )}::${sql.unsafe(types[key])} ELSE t.${sql(key)} END)`
        )
        .reduce(joinSql(sql`,\n\t `, noopSql))}
      FROM cte_values JOIN cte_where ON cte_values.__where_rank = cte_where.rank
      WHERE
        ${whereKeys
          .map((key) => key.match(/(.*)_(_.*)/))
          .map(
            ([wkey, key, op]) =>
              sql`(${sql.unsafe(
                stringifyValueForSQL(wkey)
              )} != ALL(cte_where.__where_keys) OR t.${sql(key)}::${sql.unsafe(
                types[key]
              )} ${mapCompare[op]} cte_where.${sql(wkey)})`
          )
          .reduce(joinSql(sql`\n\tAND\n\t`, noopSql))}
      RETURNING
          *
      )
      \nSELECT count(*) as affected_rows FROM cte_update
      `;
    },
    insert: pipe(
      (root) => [{ table: "root", data: { root } }],
      formatData,
      addUniqueKeys,
      (arr) =>
        !(arr?.length > 0)
          ? []
          : sql`
    WITH ${[
      ...groupByArray(arr, ({ table }) => table)
        .map(([table, arr]) => ({
          table,
          values: groupByArray(arr, (d) => JSON.stringify(d.values)).map(
            ([_, arr]) => [
              JSON.stringify(arr.map((d) => d.index)),
              ...arr[0].values,
            ]
          ),
        }))
        .map(WITH),
      ...INSERT_CTE(arr),
    ].reduce(joinSql(sql`, `), noopSql)}

    ${pipe(
      ({ table, returning } = {}) =>
        sql`SELECT ${[
          toArray(returning).filter((v) => v).length > 0
            ? null
            : sql`count(*) as affected_rows`,
          ...toArray(returning)
            .filter((v) => v && v !== "*")
            .map((d) => sql`t1.${sql(d)}`),
          toArray(returning).includes("*") ? sql`t1.*` : null,
        ]
          .filter((v) => v)
          .reduce(joinSql(sql`, `))} FROM ${sql(`cte_${table}`)} t1 JOIN ${sql(
          `cte_${table}_rn`
        )} t2 ON array_length(t2.column1, 1) = 1 AND t1.id = t2.id;`
    )(arr.find((d) => d.index.length === 1))}
  `
    ),
  };
};
