import { readFile } from "fs/promises";
import postgres from "postgres";
import createJsonQuerier from "../src/index.js";
import { noopLog, pipe } from "../src/utils.js";

const host = "postgresql://postgres:postgres@192.168.1.56:49155/postgres";
const sql = postgres(host, { debug: console.log });
const { update } = await createJsonQuerier(sql);

pipe(JSON.parse, update, noopLog)(await readFile("tests/update.json"));
