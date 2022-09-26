class Nary extends Array {}
export const pipe = (...fns) => (...x) => fns.reduce((y, fn) => y instanceof Promise ? y.then(fn) : y instanceof Nary ? fn(...y) : fn(y), x.length > 1 ? Nary.from(x) : x[0]) // prettier-ignore

export const mapAsync = (fb) => [
  async (prev, ...v) => [...(await prev), await fb(...v)],
  Promise.resolve([]),
];

export const unique = (fn) => (a, i, arr) =>
  i === arr.findIndex((b) => fn(a) === fn(b));

export const groupByReduce = (field) => (accumulator, row) => {
  const groupby = typeof field === "function" ? field(row) : field;
  const obj = Object.fromEntries(accumulator);
  obj[groupby] = obj[groupby] || [];
  obj[groupby].push(row);
  return Object.entries(obj);
};
export const groupBy = (data, field, { asList = false } = {}) => {
  const result = (data ?? []).reduce(groupByReduce(field), []);
  return !asList ? Object.fromEntries(result) : result;
};
export const groupByArray = (data, field) =>
  groupBy(data, field, { asList: true });

export const toArray = (arr) => (Array.isArray(arr) ? arr : [arr]);

export const noopLog = (v) => (console.log(v), v);

// Ported from PostgreSQL 9.2.4 source code in src/interfaces/libpq/fe-exec.c
export function escapeLiteral(str) {
  let backSlash = false;
  let out = "'";
  let i;
  let c;
  const l = str.length;

  for (i = 0; i < l; i++) {
    c = str[i];
    if (c === "'") out += c + c;
    else if (c === "\\") {
      out += c + c;
      backSlash = true;
    } else out += c;
  }
  out += "'";

  if (backSlash) out = " E" + out;

  return out;
}

export function stringifyArrayForSQL(v, options, encode) {
  const arr = v.map((x) => stringifyValueForSQL(x, options, encode));
  return "ARRAY[" + arr.join(",") + "]";
}

export const isUuid = (uuid) =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    uuid
  );

export function stringifyValueForSQL(v, options, encode) {
  if (v == null) return "null";
  if (typeof v === "boolean") return v ? "true" : "false";
  if (Array.isArray(v)) return stringifyArrayForSQL(v, options, encode);
  if (encode) v = encode(v, options || {});
  if (typeof v === "number") return "" + v;
  if (typeof v === "bigint") return v.toString();
  if (typeof v === "string" && isUuid(v))
    return escapeLiteral("" + v) + "::uuid";
  if (typeof v === "object") return escapeLiteral(JSON.stringify(v)) + "::json";
  return escapeLiteral("" + v);
}
